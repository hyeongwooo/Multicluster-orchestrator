package controller

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	unstr "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	uobj "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"

	orchestrationv1alpha1 "github.com/wnguddn777/multicluster-orchestrator/api/v1alpha1"
)

const (

	// 헬스체크용 ENV (사용자가 셋업)
	// 예: http://nats-bus.<ns>.svc.clusterset.local:8222/healthz
	fieldManager          = "orchestrator"
	orchestratorFinalizer = "orchestration.operator.io/finalizer"
	envNatsHubHealthURL   = "NATS_HUB_HEALTH_URL"
	// 핀 복구/설정 쿨다운(초) – 너무 잦은 토글 방지
	envFailoverCooldown = "FAILOVER_COOLDOWN"

	// pin annotation key
	annoPinESWT = "orchestrator.operator.io/eswt-pinned"
)

// ──────────────────────────────────────────────────────────────
// Common label for all orchestrator-managed resources
func applyCommonLabel(u *uobj.Unstructured, event string) {
	if u == nil {
		return
	}
	obj := u.Object
	md, _ := obj["metadata"].(map[string]any)
	if md == nil {
		md = map[string]any{}
		obj["metadata"] = md
	}
	labels, _ := md["labels"].(map[string]any)
	if labels == nil {
		labels = map[string]any{}
		md["labels"] = labels
	}
	labels["orchestrator.operator.io/event"] = event
}

// ──────────────────────────────────────────────────────────────
// Reconciler
// ──────────────────────────────────────────────────────────────

type OrchestratorReconciler struct {
	client.Client
	Scheme  *runtime.Scheme
	PromURL string // (미사용; PD 컨트롤러에서 사용)
}

// +kubebuilder:rbac:groups=orchestration.operator.io,resources=orchestrators,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=orchestration.operator.io,resources=orchestrators/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=orchestration.operator.io,resources=orchestrators/finalizers,verbs=update
// +kubebuilder:rbac:groups=argoproj.io,resources=eventsources;sensors;workflowtemplates,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=serving.knative.dev,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy.karmada.io,resources=propagationpolicies;overridepolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy.karmada.io,resources=clusterpropagationpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps;services;endpoints,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=argoproj.io,resources=eventbus;eventbuses,verbs=get;list;watch;create;update;patch;delete

// ──────────────────────────────────────────────────────────────
// cluster → endpoint 매핑(ConfigMap: orchestrator-domain-map in spec.namespace)
// ──────────────────────────────────────────────────────────────

type clusterEndpoint struct {
	IP   string
	Port int32
}

func (r *OrchestratorReconciler) loadDomainMap(ctx context.Context, ns string) (map[string]clusterEndpoint, error) {
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: ns, Name: "orchestrator-domain-map"}, cm); err != nil {
		return nil, err
	}

	tmp := map[string]struct {
		ip   string
		port *int32
	}{}

	for k, v := range cm.Data {
		parts := strings.SplitN(k, ".", 2)
		if len(parts) != 2 {
			continue
		}
		name, suffix := parts[0], parts[1]
		ent := tmp[name]
		switch suffix {
		case "ip":
			ent.ip = v
		case "kourier":
			if p, err := strconv.Atoi(v); err == nil {
				pp := int32(p)
				ent.port = &pp
			}
		}
		tmp[name] = ent
	}

	out := map[string]clusterEndpoint{}
	for name, ent := range tmp {
		port := int32(31370) // fallback
		if ent.port != nil {
			port = *ent.port
		}
		out[name] = clusterEndpoint{IP: ent.ip, Port: port}
	}
	return out, nil
}

// ──────────────────────────────────────────────────────────────
// Helpers: spec defaults / tiny utils
// ──────────────────────────────────────────────────────────────

func (r *OrchestratorReconciler) defaultSpec(o *orchestrationv1alpha1.Orchestrator) {
	if o.Spec.Namespace == "" {
		o.Spec.Namespace = o.Namespace
	}
	if o.Spec.EventName == "" {
		o.Spec.EventName = o.Name
	}
	if o.Spec.EventType == "" {
		o.Spec.EventType = "webhook"
	}
	if o.Spec.EventSource == nil {
		o.Spec.EventSource = &orchestrationv1alpha1.EventSourceSpec{}
	}
	if o.Spec.EventSource.Type == "" {
		o.Spec.EventSource.Type = "webhook"
	}
	if o.Spec.EventSource.Port == 0 {
		rand.Seed(time.Now().UnixNano())
		o.Spec.EventSource.Port = int32(10000 + rand.Intn(10000))
	}
	if o.Spec.EventSource.Params == nil {
		o.Spec.EventSource.Params = map[string]string{}
	}
	if o.Spec.EventSource.Params["endpoint"] == "" {
		o.Spec.EventSource.Params["endpoint"] = "/" + o.Spec.EventName
	}
	if o.Spec.EventSource.Params["method"] == "" {
		o.Spec.EventSource.Params["method"] = "POST"
	}
	if o.Spec.Service.DomainSuffix == "" {
		o.Spec.Service.DomainSuffix = "example.com"
	}
	if o.Spec.Service.ConcurrencyTarget != nil && *o.Spec.Service.ConcurrencyTarget < 1 {
		v := int32(1)
		o.Spec.Service.ConcurrencyTarget = &v
	}
	if o.Spec.Service.ConcurrencyTarget == nil {
		v := int32(10)
		o.Spec.Service.ConcurrencyTarget = &v
	}
}

func toEnvList(m map[string]string) []any {
	if len(m) == 0 {
		return []any{}
	}
	out := make([]any, 0, len(m))
	for k, v := range m {
		out = append(out, map[string]any{"name": k, "value": v})
	}
	return out
}

func extractClusters(sel []orchestrationv1alpha1.SelectedCluster) []string {
	out := make([]string, 0, len(sel))
	for _, s := range sel {
		out = append(out, s.Cluster)
	}
	return out
}

func toAnySlice(ss []string) []any {
	out := make([]any, len(ss))
	for i := range ss {
		out[i] = ss[i]
	}
	return out
}

// failover 쿨다운(기본 30초)
func failoverCooldown() time.Duration {
	if v := strings.TrimSpace(os.Getenv(envFailoverCooldown)); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return 30 * time.Second
}

// 허브 헬스체크 (HTTP 200 기대)
func checkHubHealthy(ctx context.Context) bool {
	url := strings.TrimSpace(os.Getenv(envNatsHubHealthURL))
	if url == "" {
		// URL이 없으면 헬스체크를 패스(healthy로 간주) — 사용자 설정 필요
		return true
	}
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

// ──────────────────────────────────────────────────────────────
// Reconcile
// ──────────────────────────────────────────────────────────────

func (r *OrchestratorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var orch orchestrationv1alpha1.Orchestrator
	if err := r.Get(ctx, req.NamespacedName, &orch); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	log := crlog.FromContext(ctx)
	log.Info("Reconciling Orchestrator(managed+failover)", "name", orch.Name, "namespace", orch.Namespace)

	// Finalizer 보장 / 삭제 처리
	if orch.ObjectMeta.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(&orch, orchestratorFinalizer) {
			controllerutil.AddFinalizer(&orch, orchestratorFinalizer)
			if err := r.Update(ctx, &orch); err != nil {
				return ctrl.Result{}, err
			}
		}
	} else {
		// 자식 리소스 정리
		_ = r.deleteChild(ctx, orch.Spec.Namespace, nameKS(orch.Spec.EventName), "serving.knative.dev/v1", "Service")
		_ = r.deleteChild(ctx, orch.Spec.Namespace, nameES(orch.Spec.EventName), "argoproj.io/v1alpha1", "EventSource")
		_ = r.deleteChild(ctx, orch.Spec.Namespace, nameSN(orch.Spec.EventName), "argoproj.io/v1alpha1", "Sensor")
		_ = r.deleteChild(ctx, orch.Spec.Namespace, nameWT(orch.Spec.EventName), "argoproj.io/v1alpha1", "WorkflowTemplate")
		_ = r.deleteChild(ctx, orch.Spec.Namespace, namePP(orch.Spec.EventName), "policy.karmada.io/v1alpha1", "PropagationPolicy")
		_ = r.deleteChild(ctx, orch.Spec.Namespace, fmt.Sprintf("%s-wt-url-override", orch.Spec.EventName), "policy.karmada.io/v1alpha1", "OverridePolicy")
		_ = r.deleteChild(ctx, orch.Spec.Namespace, namePPESWT(orch.Spec.EventName), "policy.karmada.io/v1alpha1", "PropagationPolicy")
		_ = r.deleteChild(ctx, orch.Spec.Namespace, namePPOthers(orch.Spec.EventName), "policy.karmada.io/v1alpha1", "PropagationPolicy")
		_ = r.deleteChild(ctx, orch.Spec.Namespace, namePPEBAll(orch.Spec.EventName), "policy.karmada.io/v1alpha1", "PropagationPolicy")
		_ = r.deleteChild(ctx, "", fmt.Sprintf("%s-cpp-ns-%s", orch.Spec.EventName, orch.Spec.Namespace), "policy.karmada.io/v1alpha1", "ClusterPropagationPolicy")

		controllerutil.RemoveFinalizer(&orch, orchestratorFinalizer)
		_ = r.Update(ctx, &orch)
		return ctrl.Result{}, nil
	}

	// 0) 기본값 주입
	r.defaultSpec(&orch)

	// 1) 베이스 리소스(ksvc / eventsource) 생성
	ksvcURL, err := r.ensureKnativeServiceAndURL(ctx, &orch)
	if err != nil {
		return r.fail(&orch, "EnsureKsvcFailed", err)
	}

	// Ksvc 실제 URL/Ready 반영
	url, ready, _ := r.readKsvcURL(ctx, &orch)
	if ready && url != "" {
		ksvcURL = url
		r.setCond(&orch, "KnativeReady", metav1.ConditionTrue, "Applied", ksvcURL)
	} else {
		r.setCond(&orch, "KnativeReady", metav1.ConditionUnknown, "Pending", "Waiting Knative URL")
	}

	if _, err := r.ensureEventSourceNodePort(ctx, &orch); err != nil {
		return r.fail(&orch, "EnsureEventSourceFailed", err)
	}
	np, _ := r.readEventSourceNodePort(ctx, &orch)
	if np > 0 {
		orch.Status.ESNodePort = np
		r.setCond(&orch, "EventSourceServiceReady", metav1.ConditionTrue, "Applied", fmt.Sprintf("NodePort=%d", np))
	} else {
		r.setCond(&orch, "EventSourceServiceReady", metav1.ConditionUnknown, "Pending", "Waiting NodePort allocation")
	}

	// 2) PlacementDecision 확보(없으면 생성만) → 선택 기다림
	pd, err := r.ensurePlacement(ctx, &orch)
	if err != nil {
		return r.fail(&orch, "EnsurePlacementFailed", err)
	}
	if len(pd.Status.Selected) == 0 {
		orch.Status.KsvcURL = ksvcURL
		now := metav1.Now()
		orch.Status.LastPlacementTime = &now

		switch {
		case !ready || ksvcURL == "":
			r.setPhase(&orch, orchestrationv1alpha1.PhaseWaitingOnKsvc, "Waiting for Knative URL")
		case orch.Status.ESNodePort == 0:
			r.setPhase(&orch, orchestrationv1alpha1.PhaseWaitingOnNodePort, "Waiting for EventSource NodePort")
		default:
			r.setPhase(&orch, orchestrationv1alpha1.PhasePending, "Base resources created; waiting for PlacementDecision")
		}

		_ = r.Status().Update(ctx, &orch)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// 3) 장애 감지 & ES/WT 고정 핀 처리
	// 허브/엣지 후보 계산
	selected := extractClusters(pd.Status.Selected)
	hub := "" // 기본 허브: 첫번째 선택
	if len(selected) > 0 {
		hub = selected[0]
	}
	edge := "" // 허브 외 첫번째
	for _, c := range selected {
		if c != hub {
			edge = c
			break
		}
	}

	selectedAll := r.targetClustersForShared(ctx, &orch, pd)
	cppNS := renderClusterPropagationPolicyNamespace(&orch, selectedAll)
	if err := r.applyAll(ctx, []*uobj.Unstructured{cppNS}); err != nil {
		return r.fail(&orch, "ApplyNamespaceCPPFailed", err)
	}

	// (3) EventBus 자체 생성 (관리 클러스터에 정의)
	ebManaged := renderEventBusManaged(&orch)
	applyCommonLabel(ebManaged, orch.Spec.EventName)
	setOwner(&orch, ebManaged, r.Scheme)
	if err := r.applyAll(ctx, []*uobj.Unstructured{ebManaged}); err != nil {
		return r.fail(&orch, "ApplyEventBusManagedFailed", err)
	}

	// (4) EventBus 전파 PP - 네임스페이스 스코프 (OwnerRef OK)
	ppEBAll := renderPropagationPolicyEventBusAll(&orch, selectedAll)
	applyCommonLabel(ppEBAll, orch.Spec.EventName)
	setOwner(&orch, ppEBAll, r.Scheme)
	if err := r.applyAll(ctx, []*uobj.Unstructured{ppEBAll}); err != nil {
		return r.fail(&orch, "ApplyEventBusPPAllFailed", err)
	}

	now := time.Now()
	lastChange := orch.Status.LastPlacementTime
	cooldown := failoverCooldown()

	currentPin := ""
	if orch.Annotations != nil {
		currentPin = strings.TrimSpace(orch.Annotations[annoPinESWT])
	}

	hubHealthy := checkHubHealthy(ctx)

	// 핀 토글은 쿨다운 고려
	canToggle := true
	if lastChange != nil && now.Sub(lastChange.Time) < cooldown {
		canToggle = false
	}

	// 장애 → edge로 pin
	if !hubHealthy && edge != "" && currentPin != edge && canToggle {
		patch := orch.DeepCopy()
		if patch.Annotations == nil {
			patch.Annotations = map[string]string{}
		}
		patch.Annotations[annoPinESWT] = edge
		if err := r.Patch(ctx, patch, client.MergeFrom(&orch)); err == nil {
			t := metav1.Now()
			orch.Status.LastPlacementTime = &t
			_ = r.Status().Update(ctx, &orch)
			log.Info("Failover pin set", "edge", edge)
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
	}

	// 복구 → pin 해제
	if hubHealthy && currentPin != "" && canToggle {
		patch := orch.DeepCopy()
		delete(patch.Annotations, annoPinESWT)
		if err := r.Patch(ctx, patch, client.MergeFrom(&orch)); err == nil {
			t := metav1.Now()
			orch.Status.LastPlacementTime = &t
			_ = r.Status().Update(ctx, &orch)
			log.Info("Failback: pin cleared")
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
	}

	// 4) 정책 생성
	// ES/WT/Others 배치(핀 반영) + WT URL Override
	policyObjs := r.renderPoliciesESWTAndOthers(&orch, pd)
	for _, o := range policyObjs {
		setOwner(&orch, o, r.Scheme)
	}
	if err := r.applyAll(ctx, policyObjs); err != nil {
		return r.fail(&orch, "ApplyPoliciesFailed", err)
	}

	// 6) 베이스(ksvc/wt/sensor) 생성/패치 – Sensor는 항상 eventBusName=default(Managed)
	baseObjs := r.renderBase(&orch, ksvcURL, "default")
	for _, o := range baseObjs {
		setOwner(&orch, o, r.Scheme)
	}
	if err := r.applyAll(ctx, baseObjs); err != nil {
		return r.fail(&orch, "ApplyBaseFailed", err)
	}

	// 7) Status 최종 업데이트
	orch.Status.KsvcURL = ksvcURL
	orch.Status.SelectedClusters = selected
	// 핀이 있으면 failover 활성화로 간주
	pinned := ""
	if orch.Annotations != nil {
		pinned = strings.TrimSpace(orch.Annotations[annoPinESWT])
	}
	if pinned != "" {
		r.setPhase(&orch, orchestrationv1alpha1.PhaseReady, fmt.Sprintf("Failover active (pinned to %s)", pinned))
	} else {
		switch {
		case !ready || ksvcURL == "":
			r.setPhase(&orch, orchestrationv1alpha1.PhaseWaitingOnKsvc, "Waiting for Knative URL")
		case orch.Status.ESNodePort == 0:
			r.setPhase(&orch, orchestrationv1alpha1.PhaseWaitingOnNodePort, "Waiting for EventSource NodePort")
		default:
			r.setPhase(&orch, orchestrationv1alpha1.PhaseReady, "All resources applied")
		}
	}

	if err := r.Status().Update(ctx, &orch); err != nil {
		return ctrl.Result{}, err
	}

	log.V(1).Info("reconciled orchestrator (managed+failover)",
		"name", orch.Name, "ns", orch.Namespace,
		"hubHealthy", hubHealthy,
		"pinned", currentPin,
	)
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// ──────────────────────────────────────────────────────────────
// ensure / apply / delete / status helpers
// ──────────────────────────────────────────────────────────────

func (r *OrchestratorReconciler) ensurePlacement(ctx context.Context, orch *orchestrationv1alpha1.Orchestrator) (*orchestrationv1alpha1.PlacementDecision, error) {
	name := fmt.Sprintf("%s-pd", orch.Name)
	var pd orchestrationv1alpha1.PlacementDecision
	err := r.Get(ctx, client.ObjectKey{Namespace: orch.Namespace, Name: name}, &pd)
	if client.IgnoreNotFound(err) != nil {
		return nil, err
	}
	if err != nil {
		pd = orchestrationv1alpha1.PlacementDecision{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: orch.Namespace,
			},
		}
		if err := controllerutil.SetControllerReference(orch, &pd, r.Scheme); err != nil {
			return nil, err
		}
		if err := r.Create(ctx, &pd); err != nil {
			return nil, err
		}
	}
	return &pd, nil
}

func setOwner(orch *orchestrationv1alpha1.Orchestrator, o *uobj.Unstructured, scheme *runtime.Scheme) {
	_ = controllerutil.SetControllerReference(orch, o, scheme)
}

func (r *OrchestratorReconciler) applyAll(ctx context.Context, objs []*uobj.Unstructured) error {
	for _, o := range objs {
		if o == nil {
			continue
		}
		o.SetManagedFields(nil)
		if err := r.Patch(ctx, o, client.Apply,
			client.FieldOwner(fieldManager),
			client.ForceOwnership,
		); err != nil {
			return err
		}
	}
	return nil
}

func (r *OrchestratorReconciler) deleteChild(ctx context.Context, ns, name, api, kind string) error {
	u := &uobj.Unstructured{}
	u.SetAPIVersion(api)
	u.SetKind(kind)
	u.SetNamespace(ns)
	u.SetName(name)
	return r.Delete(ctx, u)
}

func (r *OrchestratorReconciler) setPhase(orch *orchestrationv1alpha1.Orchestrator, phase orchestrationv1alpha1.OrchestratorPhase, msg string) {
	orch.Status.Phase = phase
	r.setCond(orch, "Ready", condFromPhase(phase), string(phase), msg)
}

// 모든 멤버로 전파가 필요한 리소스용: PD Candidates > 도메인맵 키 > Selected
func (r *OrchestratorReconciler) targetClustersForShared(ctx context.Context, orch *orchestrationv1alpha1.Orchestrator, pd *orchestrationv1alpha1.PlacementDecision) []string {

	// 2) 도메인맵 키 전체 사용
	if domap, err := r.loadDomainMap(ctx, orch.Spec.Namespace); err == nil && len(domap) > 0 {
		out := make([]string, 0, len(domap))
		for name := range domap {
			out = append(out, name)
		}
		return out
	}

	// 3) 최후의 수단: Selected
	return extractClusters(pd.Status.Selected)
}

func condFromPhase(p orchestrationv1alpha1.OrchestratorPhase) metav1.ConditionStatus {
	switch p {
	case orchestrationv1alpha1.PhaseReady:
		return metav1.ConditionTrue
	case orchestrationv1alpha1.PhaseError:
		return metav1.ConditionFalse
	default:
		return metav1.ConditionUnknown
	}
}

func (r *OrchestratorReconciler) setCond(orch *orchestrationv1alpha1.Orchestrator, t string, status metav1.ConditionStatus, reason, msg string) {
	cond := metav1.Condition{
		Type:               t,
		Status:             status,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: orch.Generation,
		LastTransitionTime: metav1.Now(),
	}
	meta.SetStatusCondition(&orch.Status.Conditions, cond)
}

func (r *OrchestratorReconciler) fail(orch *orchestrationv1alpha1.Orchestrator, reason string, err error) (ctrl.Result, error) {
	r.setPhase(orch, orchestrationv1alpha1.PhaseError, err.Error())
	_ = r.Status().Update(context.Background(), orch)
	return ctrl.Result{RequeueAfter: 10 * time.Second}, err
}

// ──────────────────────────────────────────────────────────────
// Renderers: KService / EventSource / WorkflowTemplate / Sensor
// ──────────────────────────────────────────────────────────────

func (r *OrchestratorReconciler) ensureKnativeServiceAndURL(ctx context.Context, orch *orchestrationv1alpha1.Orchestrator) (string, error) {
	ksvc := renderKService(orch)
	applyCommonLabel(ksvc, orch.Spec.EventName)
	setOwner(orch, ksvc, r.Scheme)
	if err := r.applyAll(ctx, []*uobj.Unstructured{ksvc}); err != nil {
		return "", err
	}

	for i := 0; i < 30; i++ {
		url, ready, err := r.readKsvcURL(ctx, orch)
		if err == nil && ready && url != "" {
			return url, nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Sprintf("http://%s.%s.svc", nameKS(orch.Spec.EventName), orch.Spec.Namespace), nil
}

func renderKService(orch *orchestrationv1alpha1.Orchestrator) *uobj.Unstructured {
	container := map[string]any{
		"image": orch.Spec.Service.Image,
	}
	if env := toEnvList(orch.Spec.Service.Env); len(env) > 0 {
		container["env"] = env
	}

	spec := map[string]any{
		"template": map[string]any{
			"spec": map[string]any{
				"containers": []any{container},
			},
		},
	}

	if orch.Spec.Service.ConcurrencyTarget != nil {
		tmpl := spec["template"].(map[string]any)
		if tmpl["metadata"] == nil {
			tmpl["metadata"] = map[string]any{}
		}
		md := tmpl["metadata"].(map[string]any)
		md["annotations"] = map[string]any{
			"autoscaling.knative.dev/target": fmt.Sprintf("%d", *orch.Spec.Service.ConcurrencyTarget),
		}
	}

	u := &uobj.Unstructured{}
	u.Object = map[string]any{
		"apiVersion": "serving.knative.dev/v1",
		"kind":       "Service",
		"metadata": map[string]any{
			"name":      nameKS(orch.Spec.EventName),
			"namespace": orch.Spec.Namespace,
		},
		"spec": spec,
	}
	return u
}

func renderEventSource(orch *orchestrationv1alpha1.Orchestrator) *uobj.Unstructured {
	name := nameES(orch.Spec.EventName)
	port := orch.Spec.EventSource.Port

	u := &uobj.Unstructured{}
	u.Object = map[string]any{
		"apiVersion": "argoproj.io/v1alpha1",
		"kind":       "EventSource",
		"metadata": map[string]any{
			"name":      name,
			"namespace": orch.Spec.Namespace,
		},
		"spec": map[string]any{
			"service": map[string]any{
				"ports": []any{
					map[string]any{"port": port, "targetPort": port},
				},
			},
			"webhook": map[string]any{
				orch.Spec.EventName: map[string]any{
					"port":     fmt.Sprintf("%d", port),
					"endpoint": orch.Spec.EventSource.Params["endpoint"],
					"method":   orch.Spec.EventSource.Params["method"],
				},
			},
		},
	}
	return u
}

func renderEventSourceNodePortService(orch *orchestrationv1alpha1.Orchestrator) *uobj.Unstructured {
	if orch.Spec.EventSource.Port == 0 {
		return nil
	}
	name := nameESNodePortSvc(orch.Spec.EventName)
	port := orch.Spec.EventSource.Port

	portMap := map[string]any{
		"name":       "http",
		"port":       port,
		"targetPort": port,
	}
	if orch.Spec.EventSource.NodePort > 0 {
		portMap["nodePort"] = orch.Spec.EventSource.NodePort
	}

	spec := map[string]any{
		"type": string(corev1.ServiceTypeNodePort),
		"selector": map[string]any{
			"eventsource-name": nameES(orch.Spec.EventName),
		},
		"ports": []any{portMap},
	}

	md := map[string]any{
		"name":      name,
		"namespace": orch.Spec.Namespace,
	}

	u := &uobj.Unstructured{}
	u.Object = map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata":   md,
		"spec":       spec,
	}
	return u
}

func renderWorkflowTemplate(orch *orchestrationv1alpha1.Orchestrator, ksvcURL string) *uobj.Unstructured {
	name := nameWT(orch.Spec.EventName)

	u := &uobj.Unstructured{}
	u.Object = map[string]any{
		"apiVersion": "argoproj.io/v1alpha1",
		"kind":       "WorkflowTemplate",
		"metadata": map[string]any{
			"name":      name,
			"namespace": orch.Spec.Namespace,
		},
		"spec": map[string]any{
			"templates": []any{
				map[string]any{
					"name": "main",
					"steps": []any{
						[]any{
							map[string]any{
								"name":     "call-func",
								"template": "call-func",
							},
						},
					},
				},
				map[string]any{
					"name": "call-func",
					"http": map[string]any{
						"url":    ksvcURL,
						"method": "GET",
						"headers": []any{
							map[string]any{"name": "Content-Type", "value": "application/json"},
						},
					},
				},
			},
		},
	}
	return u
}

func renderSensor(orch *orchestrationv1alpha1.Orchestrator, eventBusName string) *uobj.Unstructured {
	sensorName := nameSN(orch.Spec.EventName)
	wtName := nameWT(orch.Spec.EventName)

	wfObj := map[string]any{
		"apiVersion": "argoproj.io/v1alpha1",
		"kind":       "Workflow",
		"metadata":   map[string]any{"generateName": fmt.Sprintf("%s-", orch.Spec.EventName)},
		"spec": map[string]any{
			"serviceAccountName":  "operate-workflow-sa",
			"entrypoint":          "main",
			"workflowTemplateRef": map[string]any{"name": wtName},
		},
	}

	dependencies := []any{
		map[string]any{
			"name":                 orch.Spec.EventName,
			"eventSourceName":      nameES(orch.Spec.EventName),
			"eventSourceNamespace": orch.Spec.Namespace,
			"eventName":            orch.Spec.EventName,
		},
	}

	trigger := map[string]any{
		"template": map[string]any{
			"name": sensorName + "-trigger",
			"argoWorkflow": map[string]any{
				"operation": "submit",
				"source":    map[string]any{"resource": wfObj},
			},
		},
	}
	if strings.TrimSpace(orch.Spec.EventLogic) != "" {
		trigger["conditions"] = orch.Spec.EventLogic
	}

	spec := map[string]any{
		"template": map[string]any{
			"eventBusName":       eventBusName, // "default" (managed)
			"serviceAccountName": "operate-workflow-sa",
		},
		"dependencies": dependencies,
		"triggers":     []any{trigger},
	}

	u := &uobj.Unstructured{}
	u.Object = map[string]any{
		"apiVersion": "argoproj.io/v1alpha1",
		"kind":       "Sensor",
		"metadata": map[string]any{
			"name":      sensorName,
			"namespace": orch.Spec.Namespace,
		},
		"spec": spec,
	}
	return u
}

// ──────────────────────────────────────────────────────────────
// Policies
// ──────────────────────────────────────────────────────────────

// ES/WT 및 Others 정책 + WT URL OP
func (r *OrchestratorReconciler) renderPoliciesESWTAndOthers(
	orch *orchestrationv1alpha1.Orchestrator,
	pd *orchestrationv1alpha1.PlacementDecision,
) []*uobj.Unstructured {
	pin := ""
	if orch.Annotations != nil {
		pin = strings.TrimSpace(orch.Annotations[annoPinESWT])
	}

	objs := []*uobj.Unstructured{}

	// ES/WT 전용 PP (핀 적용)
	ppESWT := renderPropagationPolicyESWT(orch, pd, pin)
	applyCommonLabel(ppESWT, orch.Spec.EventName)
	objs = append(objs, ppESWT)

	// Others(KSvc/Sensor/ES NodePort) 전용 PP
	ppOthers := renderPropagationPolicyOthers(orch, pd, pin)
	applyCommonLabel(ppOthers, orch.Spec.EventName)
	objs = append(objs, ppOthers)

	// WT URL OverridePolicy
	if domap, err := r.loadDomainMap(context.Background(), orch.Spec.Namespace); err == nil {
		if op := renderOverridePolicyForWT(orch, pd, domap, pin); op != nil {
			applyCommonLabel(op, orch.Spec.EventName)
			objs = append(objs, op)
		}
	}
	return objs
}

// ES/WT 전용 PropagationPolicy (핀 주석이 있으면 해당 클러스터로 고정)
func renderPropagationPolicyESWT(
	orch *orchestrationv1alpha1.Orchestrator,
	pd *orchestrationv1alpha1.PlacementDecision,
	pin string,
) *uobj.Unstructured {
	name := namePPESWT(orch.Spec.EventName)
	var selected []string
	if pin != "" {
		selected = []string{pin}
	} else {
		selected = extractClusters(pd.Status.Selected)
	}

	u := &uobj.Unstructured{}
	u.Object = map[string]any{
		"apiVersion": "policy.karmada.io/v1alpha1",
		"kind":       "PropagationPolicy",
		"metadata": map[string]any{
			"name":      name,
			"namespace": orch.Spec.Namespace,
		},
		"spec": map[string]any{
			"resourceSelectors": []any{
				map[string]any{"apiVersion": "argoproj.io/v1alpha1", "kind": "WorkflowTemplate", "name": nameWT(orch.Spec.EventName), "namespace": orch.Spec.Namespace},
				map[string]any{"apiVersion": "argoproj.io/v1alpha1", "kind": "EventSource", "name": nameES(orch.Spec.EventName), "namespace": orch.Spec.Namespace},
			},
			"placement": map[string]any{
				"clusterAffinity": map[string]any{
					"clusterNames": toAnySlice(selected),
				},
			},
		},
	}
	return u
}

func renderClusterPropagationPolicyNamespace(
	orch *orchestrationv1alpha1.Orchestrator,
	targetClusters []string,
) *uobj.Unstructured {
	u := &uobj.Unstructured{}
	u.Object = map[string]any{
		"apiVersion": "policy.karmada.io/v1alpha1",
		"kind":       "ClusterPropagationPolicy",
		"metadata": map[string]any{
			// 클러스터 스코프이므로 고유 이름을 하나 정해줍니다.
			"name": fmt.Sprintf("%s-cpp-ns-%s", orch.Spec.EventName, orch.Spec.Namespace),
		},
		"spec": map[string]any{
			"resourceSelectors": []any{
				map[string]any{
					"apiVersion": "v1",
					"kind":       "Namespace",
					"name":       orch.Spec.Namespace,
				},
			},
			"placement": map[string]any{
				"clusterAffinity": map[string]any{
					"clusterNames": toAnySlice(targetClusters),
				},
			},
		},
	}
	return u
}

// Others(KSvc/Sensor/ES NodePort) 전용 PropagationPolicy (핀 적용)
func renderPropagationPolicyOthers(
	orch *orchestrationv1alpha1.Orchestrator,
	pd *orchestrationv1alpha1.PlacementDecision,
	pin string,
) *uobj.Unstructured {
	name := namePPOthers(orch.Spec.EventName)

	var selected []string
	if pin != "" {
		selected = []string{pin}
	} else {
		selected = extractClusters(pd.Status.Selected)
	}

	u := &uobj.Unstructured{}
	u.Object = map[string]any{
		"apiVersion": "policy.karmada.io/v1alpha1",
		"kind":       "PropagationPolicy",
		"metadata": map[string]any{
			"name":      name,
			"namespace": orch.Spec.Namespace,
		},
		"spec": map[string]any{
			"resourceSelectors": []any{
				map[string]any{"apiVersion": "serving.knative.dev/v1", "kind": "Service", "name": nameKS(orch.Spec.EventName), "namespace": orch.Spec.Namespace},
				map[string]any{"apiVersion": "argoproj.io/v1alpha1", "kind": "Sensor", "name": nameSN(orch.Spec.EventName), "namespace": orch.Spec.Namespace},
				map[string]any{"apiVersion": "v1", "kind": "Service", "name": nameESNodePortSvc(orch.Spec.EventName), "namespace": orch.Spec.Namespace},
			},
			"placement": map[string]any{
				"clusterAffinity": map[string]any{
					"clusterNames": toAnySlice(selected),
				},
			},
		},
	}
	return u
}

// Managed EventBus(default) – 제어plane에서 정의만(전파는 PP로)
func renderEventBusManaged(orch *orchestrationv1alpha1.Orchestrator) *uobj.Unstructured {
	u := &uobj.Unstructured{}
	u.Object = map[string]any{
		"apiVersion": "argoproj.io/v1alpha1",
		"kind":       "EventBus",
		"metadata": map[string]any{
			"name":      "default",
			"namespace": orch.Spec.Namespace,
		},
		"spec": map[string]any{
			"jetstream": map[string]any{
				"version": "latest",
			},
		},
	}
	return u
}

// EventBus(default) – 모든 선택 클러스터에 배포
func renderPropagationPolicyEventBusAll(
	orch *orchestrationv1alpha1.Orchestrator,
	targetClusters []string,
) *uobj.Unstructured {
	u := &uobj.Unstructured{}
	u.Object = map[string]any{
		"apiVersion": "policy.karmada.io/v1alpha1",
		"kind":       "PropagationPolicy",
		"metadata": map[string]any{
			"name":      namePPEBAll(orch.Spec.EventName),
			"namespace": orch.Spec.Namespace,
		},
		"spec": map[string]any{
			"resourceSelectors": []any{
				map[string]any{
					"apiVersion": "argoproj.io/v1alpha1",
					"kind":       "EventBus",
					"name":       "default",
					"namespace":  orch.Spec.Namespace,
				},
			},
			"placement": map[string]any{
				"clusterAffinity": map[string]any{
					"clusterNames": toAnySlice(targetClusters),
				},
			},
		},
	}
	return u
}

func renderOverridePolicyForWT(
	orch *orchestrationv1alpha1.Orchestrator,
	pd *orchestrationv1alpha1.PlacementDecision,
	domap map[string]clusterEndpoint,
	pin string,
) *uobj.Unstructured {
	u := &uobj.Unstructured{}
	u.SetAPIVersion("policy.karmada.io/v1alpha1")
	u.SetKind("OverridePolicy")
	u.SetName(fmt.Sprintf("%s-wt-url-override", orch.Spec.EventName))
	u.SetNamespace(orch.Spec.Namespace)

	// ES/WT가 배치되는 집합(핀 반영)
	var selected []string
	if pin != "" {
		selected = []string{pin}
	} else {
		selected = extractClusters(pd.Status.Selected)
	}

	rules := []any{}
	for _, c := range selected {
		ep, ok := domap[c]
		if !ok || ep.IP == "" || ep.Port == 0 {
			continue
		}
		finalURL := fmt.Sprintf("http://%s.%s.%s.nip.io:%d",
			nameKS(orch.Spec.EventName), orch.Spec.Namespace, ep.IP, ep.Port)

		rules = append(rules, map[string]any{
			"targetCluster": map[string]any{
				"clusterNames": []any{c},
			},
			"overriders": map[string]any{
				"plaintext": []any{
					map[string]any{
						"path":     "/spec/templates/1/http/url",
						"operator": "replace",
						"value":    finalURL,
					},
				},
			},
		})
	}

	u.Object = map[string]any{
		"apiVersion": "policy.karmada.io/v1alpha1",
		"kind":       "OverridePolicy",
		"metadata": map[string]any{
			"name":      fmt.Sprintf("%s-wt-url-override", orch.Spec.EventName),
			"namespace": orch.Spec.Namespace,
		},
		"spec": map[string]any{
			"resourceSelectors": []any{
				map[string]any{
					"apiVersion": "argoproj.io/v1alpha1",
					"kind":       "WorkflowTemplate",
					"name":       nameWT(orch.Spec.EventName),
					"namespace":  orch.Spec.Namespace,
				},
			},
			"overrideRules": rules,
		},
	}
	return u
}

// ──────────────────────────────────────────────────────────────
// Ksvc / NodePort
// ──────────────────────────────────────────────────────────────

// --- [ADD] EventSource + NodePort 보장 메서드 ---
func (r *OrchestratorReconciler) ensureEventSourceNodePort(
	ctx context.Context,
	orch *orchestrationv1alpha1.Orchestrator,
) (int32, error) {
	es := renderEventSource(orch)
	applyCommonLabel(es, orch.Spec.EventName)
	setOwner(orch, es, r.Scheme)
	if err := r.applyAll(ctx, []*uobj.Unstructured{es}); err != nil {
		return 0, err
	}

	svc := renderEventSourceNodePortService(orch)
	if svc != nil {
		applyCommonLabel(svc, orch.Spec.EventName)
		setOwner(orch, svc, r.Scheme)
		if err := r.applyAll(ctx, []*uobj.Unstructured{svc}); err != nil {
			return 0, err
		}
	}
	// 명시 고정 NodePort가 있으면 바로 반환
	if orch.Spec.EventSource.NodePort > 0 {
		return orch.Spec.EventSource.NodePort, nil
	}
	return 0, nil
}

// --- [ADD] NodePort 읽기 메서드 ---
func (r *OrchestratorReconciler) readEventSourceNodePort(
	ctx context.Context,
	orch *orchestrationv1alpha1.Orchestrator,
) (int32, error) {
	svc := &corev1.Service{}
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: orch.Spec.Namespace,
		Name:      nameESNodePortSvc(orch.Spec.EventName),
	}, svc); err != nil {
		return 0, err
	}
	if len(svc.Spec.Ports) == 0 {
		return 0, nil
	}
	return svc.Spec.Ports[0].NodePort, nil
}

// --- [ADD] 베이스 리소스 렌더러 (WT + Sensor) ---
func (r *OrchestratorReconciler) renderBase(
	orch *orchestrationv1alpha1.Orchestrator,
	ksvcURL string,
	eventBusName string,
) []*uobj.Unstructured {
	wt := renderWorkflowTemplate(orch, ksvcURL)
	sn := renderSensor(orch, eventBusName)
	applyCommonLabel(wt, orch.Spec.EventName)
	applyCommonLabel(sn, orch.Spec.EventName)
	setOwner(orch, wt, r.Scheme)
	setOwner(orch, sn, r.Scheme)
	return []*uobj.Unstructured{wt, sn}
}

func (r *OrchestratorReconciler) readKsvcURL(ctx context.Context, orch *orchestrationv1alpha1.Orchestrator) (string, bool, error) {
	u := &uobj.Unstructured{}
	u.SetAPIVersion("serving.knative.dev/v1")
	u.SetKind("Service")
	key := client.ObjectKey{Namespace: orch.Spec.Namespace, Name: nameKS(orch.Spec.EventName)}
	if err := r.Get(ctx, key, u); err != nil {
		return "", false, err
	}

	status, found, _ := unstr.NestedMap(u.Object, "status")
	if !found || status == nil {
		return "", false, nil
	}

	url, _, _ := unstr.NestedString(u.Object, "status", "url")
	conds, _, _ := unstr.NestedSlice(u.Object, "status", "conditions")

	ready := false
	for _, c := range conds {
		if m, ok := c.(map[string]any); ok {
			if m["type"] == "Ready" && m["status"] == "True" {
				ready = true
				break
			}
		}
	}
	return url, ready, nil
}

// ──────────────────────────────────────────────────────────────
// 네이밍 규칙(일관 사용)
func nameESNodePortSvc(event string) string { return fmt.Sprintf("%s-eventsource-np", event) }
func nameES(base string) string             { return fmt.Sprintf("%s-event", base) }
func nameSN(base string) string             { return fmt.Sprintf("%s-sensor", base) }
func nameWT(base string) string             { return fmt.Sprintf("%s-wt", base) }
func nameKS(base string) string             { return fmt.Sprintf("%s-func", base) }
func namePP(base string) string             { return fmt.Sprintf("%s-pp", base) }

func namePPESWT(base string) string   { return fmt.Sprintf("%s-pp-eswt", base) }
func namePPOthers(base string) string { return fmt.Sprintf("%s-pp-others", base) }
func namePPEBAll(base string) string  { return fmt.Sprintf("%s-pp-eb-all", base) }

// SetupWithManager sets up the controller with the Manager.
// SetupWithManager
func (r *OrchestratorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&orchestrationv1alpha1.Orchestrator{}).
		Named("orchestrator").
		Complete(r)
}
