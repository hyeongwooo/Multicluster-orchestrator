// internal/controller/orchestrator_controller.go

/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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

const fieldManager = "orchestrator"
const orchestratorFinalizer = "orchestration.operator.io/finalizer"

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
		// 자식 리소스 정리 (OwnerReference가 있어도 Work/RB가 남는 경우 대비)
		_ = r.deleteChild(ctx, orch.Spec.Namespace, nameKS(orch.Spec.EventName), "serving.knative.dev/v1", "Service")
		_ = r.deleteChild(ctx, orch.Spec.Namespace, nameES(orch.Spec.EventName), "argoproj.io/v1alpha1", "EventSource")
		_ = r.deleteChild(ctx, orch.Spec.Namespace, nameSN(orch.Spec.EventName), "argoproj.io/v1alpha1", "Sensor")
		_ = r.deleteChild(ctx, orch.Spec.Namespace, nameWT(orch.Spec.EventName), "argoproj.io/v1alpha1", "WorkflowTemplate")
		_ = r.deleteChild(ctx, orch.Spec.Namespace, namePP(orch.Spec.EventName), "policy.karmada.io/v1alpha1", "PropagationPolicy")
		_ = r.deleteChild(ctx, orch.Spec.Namespace, fmt.Sprintf("%s-wt-url-override", orch.Spec.EventName), "policy.karmada.io/v1alpha1", "OverridePolicy")
		_ = r.deleteChild(ctx, orch.Spec.Namespace, "default", "argoproj.io/v1alpha1", "EventBus")
		_ = r.deleteChild(ctx, orch.Spec.Namespace, nameESNodePortSvc(orch.Spec.EventName), "v1", "Service") // 새 네이밍: <event>-eventsource-np
		_ = r.deleteChild(ctx, orch.Spec.Namespace, fmt.Sprintf("%s-es-np", orch.Name), "v1", "Service")     // 옛 네이밍: <orch.Name>-es-np (잔재 청소)

		controllerutil.RemoveFinalizer(&orch, orchestratorFinalizer)
		_ = r.Update(ctx, &orch)
		return ctrl.Result{}, nil
	}

	// 0) 기본값
	r.defaultSpec(&orch)

	// (추가) EventBus를 먼저 보장
	if err := r.ensureManagedEventBus(ctx, &orch); err != nil {
		return r.fail(&orch, "EnsureEventBusFailed", err)
	}
	// 1) 베이스 리소스(ksvc / eventsource / wt / sensor)부터 생성
	ksvcURL, err := r.ensureKnativeServiceAndURL(ctx, &orch, nil)
	if err != nil {
		return r.fail(&orch, "EnsureKsvcFailed", err)
	}
	r.setCond(&orch, "KnativeReady", metav1.ConditionTrue, "Applied", ksvcURL)

	if _, err := r.ensureEventSourceNodePort(ctx, &orch, nil); err != nil {
		return r.fail(&orch, "EnsureEventSourceFailed", err)
	}
	r.setCond(&orch, "EventSourceServiceReady", metav1.ConditionTrue, "Applied", "ok")

	baseObjs := r.renderBase(&orch, ksvcURL) // WT, Sensor
	for _, o := range baseObjs {
		setOwner(&orch, o, r.Scheme)
	}
	if err := r.applyAll(ctx, baseObjs); err != nil {
		return r.fail(&orch, "ApplyBaseFailed", err)
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
		r.setPhase(&orch, orchestrationv1alpha1.PhasePending, "Base resources created; waiting for PlacementDecision")
		_ = r.Status().Update(ctx, &orch)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// 3) PD가 선택되었으면 PP/OP 생성 (WT URL OP 포함)
	policyObjs := r.renderPolicies(&orch, pd)
	for _, o := range policyObjs {
		setOwner(&orch, o, r.Scheme)
	}
	if err := r.applyAll(ctx, policyObjs); err != nil {
		return r.fail(&orch, "ApplyPoliciesFailed", err)
	}

	// 3-1) Managed EventBus 보장 (spec.jetstream.version=latest)
	if err := r.ensureManagedEventBus(ctx, &orch); err != nil {
		return r.fail(&orch, "EnsureEventBusFailed", err)
	}

	// 4) Status 최종 업데이트
	orch.Status.KsvcURL = ksvcURL
	orch.Status.SelectedClusters = extractClusters(pd.Status.Selected)
	now := metav1.Now()
	orch.Status.LastPlacementTime = &now
	r.setPhase(&orch, orchestrationv1alpha1.PhaseReady, "All resources applied")
	if err := r.Status().Update(ctx, &orch); err != nil {
		return ctrl.Result{}, err
	}

	log.V(1).Info("reconciled orchestrator", "name", orch.Name, "ns", orch.Namespace)
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// ──────────────────────────────────────────────────────────────
// Defaults
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

// ──────────────────────────────────────────────────────────────
// KService / EventSource
// ──────────────────────────────────────────────────────────────

func setOwner(orch *orchestrationv1alpha1.Orchestrator, o *uobj.Unstructured, scheme *runtime.Scheme) {
	_ = controllerutil.SetControllerReference(orch, o, scheme)
}

func (r *OrchestratorReconciler) ensureKnativeServiceAndURL(ctx context.Context, orch *orchestrationv1alpha1.Orchestrator, _ *orchestrationv1alpha1.PlacementDecision) (string, error) {
	ksvc := renderKService(orch)
	setOwner(orch, ksvc, r.Scheme)
	if err := r.applyAll(ctx, []*uobj.Unstructured{ksvc}); err != nil {
		return "", err
	}
	return fmt.Sprintf("http://%s.%s.svc", nameKS(orch.Spec.EventName), orch.Spec.Namespace), nil
}

func (r *OrchestratorReconciler) ensureEventSourceNodePort(ctx context.Context, orch *orchestrationv1alpha1.Orchestrator, _ *orchestrationv1alpha1.PlacementDecision) (int32, error) {
	es := renderEventSource(orch)
	setOwner(orch, es, r.Scheme)
	if err := r.applyAll(ctx, []*uobj.Unstructured{es}); err != nil {
		return 0, err
	}
	svc := renderEventSourceNodePortService(orch)
	if svc != nil {
		setOwner(orch, svc, r.Scheme)
		if err := r.applyAll(ctx, []*uobj.Unstructured{svc}); err != nil {
			return 0, err
		}
	}
	if orch.Spec.EventSource.NodePort > 0 {
		return orch.Spec.EventSource.NodePort, nil
	}
	return 0, nil
}

// ──────────────────────────────────────────────────────────────
// 렌더링: 베이스/정책
// ──────────────────────────────────────────────────────────────

func (r *OrchestratorReconciler) renderBase(
	orch *orchestrationv1alpha1.Orchestrator,
	ksvcURL string,
) []*uobj.Unstructured {
	wt := renderWorkflowTemplate(orch, ksvcURL)
	sn := renderSensor(orch)
	return []*uobj.Unstructured{wt, sn}
}

func (r *OrchestratorReconciler) renderPolicies(
	orch *orchestrationv1alpha1.Orchestrator,
	pd *orchestrationv1alpha1.PlacementDecision,
) []*uobj.Unstructured {
	pp := renderPropagationPolicy(orch, pd)
	domap, err := r.loadDomainMap(context.Background(), orch.Spec.Namespace)
	if err != nil {
		return []*uobj.Unstructured{pp}
	}
	op := renderOverridePolicyForWT(orch, pd, domap)
	return []*uobj.Unstructured{pp, op}
}

// ──────────────────────────────────────────────────────────────
// Apply / Delete / Status helpers
// ──────────────────────────────────────────────────────────────

func (r *OrchestratorReconciler) applyAll(ctx context.Context, objs []*uobj.Unstructured) error {
	for _, o := range objs {
		if o == nil {
			continue
		}
		o.SetManagedFields(nil)
		if err := r.Patch(ctx, o, client.Apply, client.FieldOwner(fieldManager)); err != nil {
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
// Managed EventBus
// ──────────────────────────────────────────────────────────────

func (r *OrchestratorReconciler) ensureManagedEventBus(ctx context.Context, orch *orchestrationv1alpha1.Orchestrator) error {
	eb := renderEventBusManaged(orch)
	setOwner(orch, eb, r.Scheme)
	return r.applyAll(ctx, []*uobj.Unstructured{eb})
}

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

// ──────────────────────────────────────────────────────────────
// Renderers
// ──────────────────────────────────────────────────────────────

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
	// NodePort 노출 여부: NodePort 필드가 지정되었으면 생성
	// (0 = 랜덤 할당, >0 = 고정 NodePort)
	if orch.Spec.EventSource.Port == 0 {
		return nil // 포트 자체가 없으면 만들 필요 없음
	}

	// EventSource NodePort Service의 표준 이름
	name := nameESNodePortSvc(orch.Spec.EventName)
	port := orch.Spec.EventSource.Port

	// 공통 포트 정의
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

func renderSensor(orch *orchestrationv1alpha1.Orchestrator) *uobj.Unstructured {
	// Consistent sensor name and cross-cluster propagation compatibility
	sensorName := nameSN(orch.Spec.EventName)
	wtName := nameWT(orch.Spec.EventName)

	// Workflow object to be submitted by the Sensor (same as earlier working style)
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

	// Dependencies: include eventSourceNamespace for clarity across clusters
	dependencies := []any{
		map[string]any{
			"name":                 orch.Spec.EventName,
			"eventSourceName":      nameES(orch.Spec.EventName),
			"eventSourceNamespace": orch.Spec.Namespace,
			"eventName":            orch.Spec.EventName,
		},
	}

	// Trigger uses argoWorkflow submit to run the Workflow from the WorkflowTemplate
	triggerTmpl := map[string]any{
		"name": sensorName + "-trigger",
		"argoWorkflow": map[string]any{
			"operation": "submit",
			"source":    map[string]any{"resource": wfObj},
		},
	}
	// Add conditions only when user provided eventLogic in the CR
	if orch.Spec.EventLogic != "" {
		triggerTmpl["conditions"] = orch.Spec.EventLogic
	} else {

		triggerTmpl["conditions"] = nil
	}

	spec := map[string]any{
		"template": map[string]any{
			"eventBusName":       "default",
			"serviceAccountName": "operate-workflow-sa",
		},
		"dependencies": dependencies,
		"triggers": []any{
			map[string]any{"template": triggerTmpl},
		},
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

func renderPropagationPolicy(orch *orchestrationv1alpha1.Orchestrator, pd *orchestrationv1alpha1.PlacementDecision) *uobj.Unstructured {
	name := namePP(orch.Spec.EventName)
	selected := extractClusters(pd.Status.Selected)

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
				map[string]any{"apiVersion": "argoproj.io/v1alpha1", "kind": "WorkflowTemplate", "name": nameWT(orch.Spec.EventName), "namespace": orch.Spec.Namespace},
				map[string]any{"apiVersion": "argoproj.io/v1alpha1", "kind": "EventSource", "name": nameES(orch.Spec.EventName), "namespace": orch.Spec.Namespace},
				// NodePort Service for EventSource
				map[string]any{"apiVersion": "v1", "kind": "Service", "name": nameESNodePortSvc(orch.Spec.EventName), "namespace": orch.Spec.Namespace},
				map[string]any{"apiVersion": "argoproj.io/v1alpha1", "kind": "EventBus", "name": "default", "namespace": orch.Spec.Namespace},
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

func renderOverridePolicyForWT(
	orch *orchestrationv1alpha1.Orchestrator,
	pd *orchestrationv1alpha1.PlacementDecision,
	domap map[string]clusterEndpoint,
) *uobj.Unstructured {
	u := &uobj.Unstructured{}
	u.SetAPIVersion("policy.karmada.io/v1alpha1")
	u.SetKind("OverridePolicy")
	u.SetName(fmt.Sprintf("%s-wt-url-override", orch.Spec.EventName))
	u.SetNamespace(orch.Spec.Namespace)

	rules := []any{}
	for _, sel := range pd.Status.Selected {
		c := sel.Cluster
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
// helpers
// ──────────────────────────────────────────────────────────────

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
// 네이밍 규칙(일관 사용)
func nameESNodePortSvc(event string) string { return fmt.Sprintf("%s-eventsource-np", event) }
func nameES(base string) string             { return fmt.Sprintf("%s-event", base) }
func nameSN(base string) string             { return fmt.Sprintf("%s-sensor", base) }
func nameWT(base string) string             { return fmt.Sprintf("%s-wt", base) }
func nameKS(base string) string             { return fmt.Sprintf("%s-func", base) }
func namePP(base string) string             { return fmt.Sprintf("%s-pp", base) }

// SetupWithManager sets up the controller with the Manager.
func (r *OrchestratorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&orchestrationv1alpha1.Orchestrator{}).
		Owns(&orchestrationv1alpha1.PlacementDecision{}).
		Named("orchestrator").
		Complete(r)
}
