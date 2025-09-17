package controller

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	uobj "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"

	orchestrationv1alpha1 "github.com/wnguddn777/multicluster-orchestrator/api/v1alpha1"
)

const (
	fieldManager          = "orchestrator"
	orchestratorFinalizer = "orchestration.operator.io/finalizer"
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
//   <cluster>.ip:       "192.168.10.x"
//   <cluster>.kourier:  "31863" (NodePort)
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

	// Back-compat: 단일 EventSource → 복수 EventSources 승격
	if len(o.Spec.EventSources) == 0 && o.Spec.EventSource != nil && o.Spec.EventSource.Name != "" {
		o.Spec.EventSources = []orchestrationv1alpha1.EventSourceSpec{*o.Spec.EventSource}
	}

	// EventSources 기본값
	if len(o.Spec.EventSources) > 0 {
		ports := randomPorts(len(o.Spec.EventSources), 10000)
		for i := range o.Spec.EventSources {
			es := &o.Spec.EventSources[i]
			if es.Type == "" {
				es.Type = "webhook"
			}
			if es.Port == 0 {
				es.Port = ports[i]
			}
			if es.Params == nil {
				es.Params = map[string]string{}
			}
			if es.Params["endpoint"] == "" {
				name := es.Name
				if name == "" {
					name = o.Spec.EventName
				}
				es.Params["endpoint"] = "/" + name
			}
			if es.Params["method"] == "" {
				es.Params["method"] = "POST"
			}
		}
	} else {
		// 레거시 단일 EventSource 기본값
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
	}

	// Services: 리스트 우선. 단일 service 있으면 승격
	if len(o.Spec.Services) == 0 && o.Spec.Service.Image != "" {
		if o.Spec.Service.Name == "" {
			o.Spec.Service.Name = nameKS(o.Spec.EventName)
		}
		o.Spec.Services = []orchestrationv1alpha1.ServiceSpec{o.Spec.Service}
	}
	for i := range o.Spec.Services {
		if o.Spec.Services[i].ConcurrencyTarget != nil && *o.Spec.Services[i].ConcurrencyTarget < 1 {
			v := int32(1)
			o.Spec.Services[i].ConcurrencyTarget = &v
		}
	}

	if o.Spec.EventType == "" {
		o.Spec.EventType = "webhook"
	}
}

func randomPorts(n int, start int32) []int32 {
	rand.Seed(time.Now().UnixNano())
	used := map[int32]bool{}
	out := make([]int32, n)
	for i := 0; i < n; i++ {
		for {
			p := start + int32(rand.Intn(10000))
			if !used[p] {
				used[p] = true
				out[i] = p
				break
			}
		}
	}
	return out
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

// ──────────────────────────────────────────────────────────────
// Reconcile
// ──────────────────────────────────────────────────────────────

func (r *OrchestratorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var orch orchestrationv1alpha1.Orchestrator
	if err := r.Get(ctx, req.NamespacedName, &orch); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	log := crlog.FromContext(ctx)
	log.Info("Reconciling Orchestrator", "name", orch.Name, "namespace", orch.Namespace)

	// Finalizer 보장 / 삭제 처리
	if orch.ObjectMeta.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(&orch, orchestratorFinalizer) {
			controllerutil.AddFinalizer(&orch, orchestratorFinalizer)
			if err := r.Update(ctx, &orch); err != nil {
				return ctrl.Result{}, err
			}
		}
	} else {
		// delete KServices
		for _, s := range orch.Spec.Services {
			_ = r.deleteChild(ctx, orch.Spec.Namespace, s.Name, "serving.knative.dev/v1", "Service")
		}
		if len(orch.Spec.Services) == 0 {
			_ = r.deleteChild(ctx, orch.Spec.Namespace, nameKS(orch.Spec.EventName), "serving.knative.dev/v1", "Service")
		}

		// delete EventSources
		if len(orch.Spec.EventSources) > 0 {
			for _, es := range orch.Spec.EventSources {
				_ = r.deleteChild(ctx, orch.Spec.Namespace, es.Name, "argoproj.io/v1alpha1", "EventSource")
				_ = r.deleteChild(ctx, orch.Spec.Namespace, es.Name+"-np", "v1", "Service")
			}
		} else {
			_ = r.deleteChild(ctx, orch.Spec.Namespace, nameES(orch.Spec.EventName), "argoproj.io/v1alpha1", "EventSource")
			_ = r.deleteChild(ctx, orch.Spec.Namespace, nameESNodePortSvc(orch.Spec.EventName), "v1", "Service")
		}

		// sensor / workflowtemplate / policies
		_ = r.deleteChild(ctx, orch.Spec.Namespace, nameSN(orch.Spec.EventName), "argoproj.io/v1alpha1", "Sensor")
		_ = r.deleteChild(ctx, orch.Spec.Namespace, nameWT(orch.Spec.EventName), "argoproj.io/v1alpha1", "WorkflowTemplate")
		_ = r.deleteChild(ctx, orch.Spec.Namespace, namePP(orch.Spec.EventName), "policy.karmada.io/v1alpha1", "PropagationPolicy")
		_ = r.deleteChild(ctx, orch.Spec.Namespace, fmt.Sprintf("%s-wt-url-override", orch.Spec.EventName), "policy.karmada.io/v1alpha1", "OverridePolicy")
		_ = r.deleteChild(ctx, orch.Spec.Namespace, namePPESWT(orch.Spec.EventName), "policy.karmada.io/v1alpha1", "PropagationPolicy")
		_ = r.deleteChild(ctx, orch.Spec.Namespace, namePPOthers(orch.Spec.EventName), "policy.karmada.io/v1alpha1", "PropagationPolicy")
		_ = r.deleteChild(ctx, "", fmt.Sprintf("%s-cpp-ns-%s", orch.Spec.EventName, orch.Spec.Namespace), "policy.karmada.io/v1alpha1", "ClusterPropagationPolicy")

		controllerutil.RemoveFinalizer(&orch, orchestratorFinalizer)
		_ = r.Update(ctx, &orch)
		return ctrl.Result{}, nil
	}

	// 0) 기본값 주입
	r.defaultSpec(&orch)

	// 1) 베이스 리소스(KServices / EventSources) 생성
	if err := r.ensureKnativeServices(ctx, &orch); err != nil {
		return r.fail(&orch, "EnsureKsvcFailed", err)
	}
	if _, err := r.ensureEventSourceNodePort(ctx, &orch); err != nil {
		return r.fail(&orch, "EnsureEventSourceFailed", err)
	}

	// 2) PlacementDecision 확보(없으면 생성만) → 선택 기다림
	pd, err := r.ensurePlacement(ctx, &orch)
	if err != nil {
		return r.fail(&orch, "EnsurePlacementFailed", err)
	}
	if len(pd.Status.Selected) == 0 {
		now := metav1.Now()
		orch.Status.LastPlacementTime = &now
		r.setPhase(&orch, orchestrationv1alpha1.PhasePending, "Base resources created; waiting for PlacementDecision")
		_ = r.Status().Update(ctx, &orch)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// 3) 대상 클러스터 계산 (PD 선택 결과 기반)
	selected := extractClusters(pd.Status.Selected)

	// 선택된(또는 도메인맵 기반 전체) 클러스터로 네임스페이스 전파
	selectedAll := r.targetClustersForShared(ctx, &orch, pd)
	cppNS := renderClusterPropagationPolicyNamespace(&orch, selectedAll)
	if err := r.applyAll(ctx, []*uobj.Unstructured{cppNS}); err != nil {
		return r.fail(&orch, "ApplyNamespaceCPPFailed", err)
	}

	primary := []string{}
	if len(selected) > 0 {
		primary = []string{selected[0]}
	}

	svcURL := map[string]string{}
	if len(primary) > 0 {
		if domap, err := r.loadDomainMap(ctx, orch.Spec.Namespace); err == nil {
			if ep, ok := domap[primary[0]]; ok && ep.IP != "" {
				for _, s := range orch.Spec.Services {
					host := fmt.Sprintf("%s.%s.%s.nip.io", s.Name, orch.Spec.Namespace, ep.IP)
					if ep.Port > 0 {
						svcURL[s.Name] = fmt.Sprintf("http://%s:%d", host, ep.Port)
					} else {
						svcURL[s.Name] = fmt.Sprintf("http://%s", host)
					}
				}
			}
		}
	}
	// 4) 정책 생성
	//    - PrimaryOnly: WT/Sensor + EventSource/NodePort (항상 primary)
	//    - KSvc: 현재는 primary 에만 배포하도록 제한 (svcTargets = primary)
	svcTargets := primary
	policyObjs := r.renderPoliciesSplit(&orch, svcTargets, primary)
	for _, o := range policyObjs {
		setOwner(&orch, o, r.Scheme)
	}
	if err := r.applyAll(ctx, policyObjs); err != nil {
		return r.fail(&orch, "ApplyPoliciesFailed", err)
	}

	// 5) 베이스(ksvc/wt/sensor) 생성/패치 - 고정 EventBus("default") 사용
	baseObjs := r.renderBase(&orch, "default", svcURL)
	for _, o := range baseObjs {
		setOwner(&orch, o, r.Scheme)
	}
	if err := r.applyAll(ctx, baseObjs); err != nil {
		return r.fail(&orch, "ApplyBaseFailed", err)
	}

	// 6) Status 최종 업데이트
	orch.Status.SelectedClusters = selected

	// 간단한 Phase 계산
	r.setPhase(&orch, orchestrationv1alpha1.PhaseReady, "All resources applied")

	if err := r.Status().Update(ctx, &orch); err != nil {
		return ctrl.Result{}, err
	}

	log.V(1).Info("reconciled orchestrator",
		"name", orch.Name, "ns", orch.Namespace,
		"selected", selected,
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

func (r *OrchestratorReconciler) targetClustersForShared(ctx context.Context, orch *orchestrationv1alpha1.Orchestrator, pd *orchestrationv1alpha1.PlacementDecision) []string {
	// 1) 도메인맵이 있으면 키 전체 사용(전 클러스터 전파용)
	if domap, err := r.loadDomainMap(ctx, orch.Spec.Namespace); err == nil && len(domap) > 0 {
		out := make([]string, 0, len(domap))
		for name := range domap {
			out = append(out, name)
		}
		return out
	}
	// 2) 없으면 Selected만
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

func renderKServices(orch *orchestrationv1alpha1.Orchestrator) []*uobj.Unstructured {
	objs := []*uobj.Unstructured{}
	for _, svc := range orch.Spec.Services {
		container := map[string]any{
			"image": svc.Image,
		}
		if env := toEnvList(svc.Env); len(env) > 0 {
			container["env"] = env
		}
		spec := map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"containers": []any{container},
				},
			},
		}
		if svc.ConcurrencyTarget != nil {
			tmpl := spec["template"].(map[string]any)
			md, ok := tmpl["metadata"].(map[string]any)
			if !ok || md == nil {
				md = map[string]any{}
				tmpl["metadata"] = md
			}
			md["annotations"] = map[string]any{
				"autoscaling.knative.dev/target": fmt.Sprintf("%d", *svc.ConcurrencyTarget),
			}
		}
		u := &uobj.Unstructured{}
		u.Object = map[string]any{
			"apiVersion": "serving.knative.dev/v1",
			"kind":       "Service",
			"metadata": map[string]any{
				"name":      svc.Name,
				"namespace": orch.Spec.Namespace,
			},
			"spec": spec,
		}
		objs = append(objs, u)
	}
	return objs
}

func (r *OrchestratorReconciler) ensureKnativeServices(ctx context.Context, orch *orchestrationv1alpha1.Orchestrator) error {
	kobjs := renderKServices(orch)
	for _, o := range kobjs {
		applyCommonLabel(o, orch.Spec.EventName)
		setOwner(orch, o, r.Scheme)
	}
	return r.applyAll(ctx, kobjs)
}

// 기존: func renderEventSources(orch *orchestrationv1alpha1.Orchestrator) []*uobj.Unstructured {
func renderEventSources(orch *orchestrationv1alpha1.Orchestrator) []*uobj.Unstructured {
	objs := []*uobj.Unstructured{}
	for _, es := range orch.Spec.EventSources {
		endpoint := es.Params["endpoint"]
		if endpoint == "" {
			endpoint = "/" + es.Name
		}
		method := es.Params["method"]
		if method == "" {
			method = "POST"
		}
		u := &uobj.Unstructured{}
		u.Object = map[string]any{
			"apiVersion": "argoproj.io/v1alpha1",
			"kind":       "EventSource",
			"metadata": map[string]any{
				"name":      es.Name,
				"namespace": orch.Spec.Namespace, //
			},
			"spec": map[string]any{
				"service": map[string]any{
					"ports": []any{
						map[string]any{"port": es.Port, "targetPort": es.Port},
					},
				},
				"webhook": map[string]any{
					es.Name: map[string]any{
						"port":     fmt.Sprintf("%d", es.Port),
						"endpoint": endpoint,
						"method":   method,
					},
				},
			},
		}
		objs = append(objs, u)

		// 항상 NodePort Service를 생성하되, 고정 포트를 원할 때만 nodePort를 지정한다.
		svcPort := map[string]any{
			"name":       "http",
			"port":       es.Port,
			"targetPort": es.Port,
		}
		if es.NodePort > 0 {
			svcPort["nodePort"] = es.NodePort
		}

		svc := &uobj.Unstructured{}
		svc.Object = map[string]any{
			"apiVersion": "v1",
			"kind":       "Service",
			"metadata": map[string]any{
				"name":      es.Name + "-np",
				"namespace": orch.Spec.Namespace,
			},
			"spec": map[string]any{
				"type": "NodePort",
				"selector": map[string]any{
					"eventsource-name": es.Name,
				},
				"ports": []any{svcPort},
			},
		}
		objs = append(objs, svc)
	}
	return objs
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

func serviceURLFor(svcName string, orch *orchestrationv1alpha1.Orchestrator, svcURL map[string]string) string {
	if svcURL != nil {
		if v, ok := svcURL[svcName]; ok && strings.TrimSpace(v) != "" {
			return v
		}
	}
	// fallback: cluster-local
	return fmt.Sprintf("http://%s.%s.svc.cluster.local", svcName, orch.Spec.Namespace)
}

func renderWorkflowTemplate(orch *orchestrationv1alpha1.Orchestrator, svcURL map[string]string) *uobj.Unstructured {
	name := nameWT(orch.Spec.EventName)

	stepsNested := []any{}
	for _, svc := range orch.Spec.Services {
		step := map[string]any{
			"name":     svc.Name,
			"template": svc.Name,
		}
		stepsNested = append(stepsNested, []any{step})
	}

	templates := []any{
		map[string]any{
			"name":  "main",
			"steps": stepsNested,
		},
	}

	for _, svc := range orch.Spec.Services {
		baseURL := serviceURLFor(svc.Name, orch, svcURL)
		path := "/" + svc.Name
		finalURL := strings.TrimRight(baseURL, "/") + path
		tmpl := map[string]any{
			"name": svc.Name,
			"http": map[string]any{
				"url":    finalURL,
				"method": "POST",
				"headers": []any{
					map[string]any{"name": "Content-Type", "value": "application/json"},
				},
			},
		}
		templates = append(templates, tmpl)
	}

	u := &uobj.Unstructured{}
	u.Object = map[string]any{
		"apiVersion": "argoproj.io/v1alpha1",
		"kind":       "WorkflowTemplate",
		"metadata": map[string]any{
			"name":      name,
			"namespace": orch.Spec.Namespace,
		},
		"spec": map[string]any{
			"entrypoint": "main",
			"templates":  templates,
		},
	}
	return u
}

func renderLegacySensor(orch *orchestrationv1alpha1.Orchestrator, eventBusName string) *uobj.Unstructured {
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

	spec := map[string]any{
		"template": map[string]any{
			"eventBusName":       eventBusName,
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

func renderClusterPropagationPolicyNamespace(
	orch *orchestrationv1alpha1.Orchestrator,
	clusterNames []string,
) *uobj.Unstructured {
	name := fmt.Sprintf("%s-cpp-ns-%s", orch.Spec.EventName, orch.Spec.Namespace)

	u := &uobj.Unstructured{}
	u.SetAPIVersion("policy.karmada.io/v1alpha1")
	u.SetKind("ClusterPropagationPolicy")
	u.SetName(name)
	u.Object = map[string]any{
		"apiVersion": "policy.karmada.io/v1alpha1",
		"kind":       "ClusterPropagationPolicy",
		"metadata": map[string]any{
			"name": name,
		},
		"spec": map[string]any{
			"resourceSelectors": []any{
				map[string]any{"apiVersion": "v1", "kind": "Namespace", "name": orch.Spec.Namespace},
			},
			"placement": map[string]any{
				"clusterAffinity": map[string]any{
					"clusterNames": toAnySlice(clusterNames),
				},
			},
		},
	}
	return u
}

// Policies (split):
//   - PrimaryOnly: WT + Sensor + EventSource + ES NodePort (항상 primary에만 전파)
//   - Multi: KSvc (두 번째 인자에 넘긴 대상 클러스터로 전파; 현재 호출부에서는 primary만 전달)
func (r *OrchestratorReconciler) renderPoliciesSplit(
	orch *orchestrationv1alpha1.Orchestrator,
	selected []string,
	primary []string,
) []*uobj.Unstructured {
	objs := []*uobj.Unstructured{}

	// WT + Sensor -> primary only
	ppPrimary := renderPropagationPolicyPrimaryOnly(orch, primary)
	applyCommonLabel(ppPrimary, orch.Spec.EventName)
	objs = append(objs, ppPrimary)

	// KSvc + ES + ES NodePort -> selected clusters
	ppMulti := renderPropagationPolicyMulti(orch, selected)
	applyCommonLabel(ppMulti, orch.Spec.EventName)
	objs = append(objs, ppMulti)

	return objs
}

// PrimaryOnly: WT + Sensor + EventSource + NodePort Service
func renderPropagationPolicyPrimaryOnly(
	orch *orchestrationv1alpha1.Orchestrator,
	primary []string,
) *uobj.Unstructured {
	name := fmt.Sprintf("%s-pp-primary", orch.Spec.EventName)

	// WT + Sensor (must co-locate)
	selectors := []any{
		map[string]any{"apiVersion": "argoproj.io/v1alpha1", "kind": "WorkflowTemplate", "name": nameWT(orch.Spec.EventName), "namespace": orch.Spec.Namespace},
		map[string]any{"apiVersion": "argoproj.io/v1alpha1", "kind": "Sensor", "name": nameSN(orch.Spec.EventName), "namespace": orch.Spec.Namespace},
	}

	// EventSources + NodePort Services (deploy only on primary)
	if len(orch.Spec.EventSources) > 0 {
		for _, es := range orch.Spec.EventSources {
			selectors = append(selectors,
				map[string]any{"apiVersion": "argoproj.io/v1alpha1", "kind": "EventSource", "name": es.Name, "namespace": orch.Spec.Namespace},
				map[string]any{"apiVersion": "v1", "kind": "Service", "name": es.Name + "-np", "namespace": orch.Spec.Namespace},
			)
		}
	} else {
		// legacy single ES path
		selectors = append(selectors,
			map[string]any{"apiVersion": "argoproj.io/v1alpha1", "kind": "EventSource", "name": nameES(orch.Spec.EventName), "namespace": orch.Spec.Namespace},
			map[string]any{"apiVersion": "v1", "kind": "Service", "name": nameESNodePortSvc(orch.Spec.EventName), "namespace": orch.Spec.Namespace},
		)
	}

	u := &uobj.Unstructured{}
	u.Object = map[string]any{
		"apiVersion": "policy.karmada.io/v1alpha1",
		"kind":       "PropagationPolicy",
		"metadata":   map[string]any{"name": name, "namespace": orch.Spec.Namespace},
		"spec": map[string]any{
			"resourceSelectors": selectors,
			"placement":         map[string]any{"clusterAffinity": map[string]any{"clusterNames": toAnySlice(primary)}},
		},
	}
	return u
}

// Multi: KSvc (Knative Service) only
func renderPropagationPolicyMulti(
	orch *orchestrationv1alpha1.Orchestrator,
	selected []string,
) *uobj.Unstructured {
	name := fmt.Sprintf("%s-pp-multi", orch.Spec.EventName)
	selectors := []any{}

	// KServices (one or many)
	if len(orch.Spec.Services) > 0 {
		for _, s := range orch.Spec.Services {
			selectors = append(selectors, map[string]any{
				"apiVersion": "serving.knative.dev/v1", "kind": "Service", "name": s.Name, "namespace": orch.Spec.Namespace,
			})
		}
	} else {
		selectors = append(selectors, map[string]any{
			"apiVersion": "serving.knative.dev/v1", "kind": "Service", "name": nameKS(orch.Spec.EventName), "namespace": orch.Spec.Namespace,
		})
	}

	u := &uobj.Unstructured{}
	u.Object = map[string]any{
		"apiVersion": "policy.karmada.io/v1alpha1",
		"kind":       "PropagationPolicy",
		"metadata":   map[string]any{"name": name, "namespace": orch.Spec.Namespace},
		"spec": map[string]any{
			"resourceSelectors": selectors,
			"placement":         map[string]any{"clusterAffinity": map[string]any{"clusterNames": toAnySlice(selected)}},
		},
	}
	return u
}

// ──────────────────────────────────────────────────────────────
// Ksvc / NodePort
// ──────────────────────────────────────────────────────────────

func (r *OrchestratorReconciler) ensureEventSourceNodePort(
	ctx context.Context,
	orch *orchestrationv1alpha1.Orchestrator,
) (int32, error) {
	// EventSource를 EventBus가 있는 NS로 생성

	esObjs := renderEventSources(orch)
	for _, o := range esObjs {
		applyCommonLabel(o, orch.Spec.EventName)
		setOwner(orch, o, r.Scheme)
	}
	if err := r.applyAll(ctx, esObjs); err != nil {
		return 0, err
	}

	// 레거시 단일 EventSource 경로만 기존 NodePort Svc 유지
	if len(orch.Spec.EventSources) == 0 && orch.Spec.EventSource != nil {
		svc := renderEventSourceNodePortService(orch)
		if svc != nil {
			applyCommonLabel(svc, orch.Spec.EventName)
			setOwner(orch, svc, r.Scheme)
			if err := r.applyAll(ctx, []*uobj.Unstructured{svc}); err != nil {
				return 0, err
			}
		}
		if orch.Spec.EventSource.NodePort > 0 {
			return orch.Spec.EventSource.NodePort, nil
		}
	}
	return 0, nil
}

func (r *OrchestratorReconciler) readEventSourceNodePort(
	ctx context.Context,
	orch *orchestrationv1alpha1.Orchestrator,
) (int32, error) {
	if len(orch.Spec.EventSources) > 0 || orch.Spec.EventSource == nil {
		return 0, nil
	}
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

// --- 베이스 리소스 렌더러 (WT + Sensor) ---
func (r *OrchestratorReconciler) renderBase(
	orch *orchestrationv1alpha1.Orchestrator,
	eventBusName string,
	svcURL map[string]string,
) []*uobj.Unstructured {
	wt := renderWorkflowTemplate(orch, svcURL)

	var sn *uobj.Unstructured
	if len(orch.Spec.EventSources) > 0 {
		// build sensor from list of event sources using EventLogic if provided
		sensorName := nameSN(orch.Spec.EventName)
		wtName := nameWT(orch.Spec.EventName)

		dependencies := []any{}
		for _, es := range orch.Spec.EventSources {
			if strings.TrimSpace(es.Name) == "" {
				continue
			}
			dependencies = append(dependencies, map[string]any{
				"name":                 es.Name,
				"eventSourceName":      es.Name,
				"eventSourceNamespace": orch.Spec.Namespace,
				"eventName":            es.Name,
			})
		}

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
				"eventBusName":       eventBusName, // "default" 등
				"serviceAccountName": "operate-workflow-sa",
			},
			"dependencies": dependencies,
			"triggers":     []any{trigger},
		}

		sn = &uobj.Unstructured{}
		sn.Object = map[string]any{
			"apiVersion": "argoproj.io/v1alpha1",
			"kind":       "Sensor",
			"metadata": map[string]any{
				"name":      sensorName,
				"namespace": orch.Spec.Namespace, //
			},
			"spec": spec,
		}
	} else {
		// legacy single-EventSource path
		sn = renderLegacySensor(orch, eventBusName)
		// ✅ 레거시도 동일하게 Sensor NS 보정
		if md, ok := sn.Object["metadata"].(map[string]any); ok {
			md["namespace"] = orch.Spec.Namespace
		}
	}

	applyCommonLabel(wt, orch.Spec.EventName)
	applyCommonLabel(sn, orch.Spec.EventName)
	setOwner(orch, wt, r.Scheme)
	setOwner(orch, sn, r.Scheme)
	return []*uobj.Unstructured{wt, sn}
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
func namePPEBEdge(base string) string { return fmt.Sprintf("%s-pp-eb-edge", base) } // 남겨두어도 미사용

// SetupWithManager sets up the controller with the Manager.
func (r *OrchestratorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&orchestrationv1alpha1.Orchestrator{}).
		Named("orchestrator").
		Complete(r)
}
