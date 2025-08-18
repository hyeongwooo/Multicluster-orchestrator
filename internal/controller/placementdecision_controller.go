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
	"os"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"

	orchestrationv1alpha1 "github.com/wnguddn777/multicluster-orchestrator/api/v1alpha1"
)

// PlacementDecisionReconciler reconciles a PlacementDecision object
type PlacementDecisionReconciler struct {
	client.Client
	Scheme  *runtime.Scheme
	PromURL string
}

// +kubebuilder:rbac:groups=orchestration.operator.io,resources=placementdecisions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=orchestration.operator.io,resources=placementdecisions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=orchestration.operator.io,resources=placementdecisions/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the PlacementDecision object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/reconcile
// Prometheus API 응답 구조
type promAPIResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  []any             `json:"value"` // [ <unix_time>, "<string_value>" ]
		} `json:"result"`
	} `json:"data"`
}

func (r *PlacementDecisionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := crlog.FromContext(ctx)

	log.Info("PD reconcile start", "namespacedName", req.NamespacedName)

	var pd orchestrationv1alpha1.PlacementDecision
	if err := r.Get(ctx, req.NamespacedName, &pd); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !pd.ObjectMeta.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Owner Orchestrator 찾기
	var orch orchestrationv1alpha1.Orchestrator
	foundOwner := false
	for _, ow := range pd.OwnerReferences {
		if ow.Kind == "Orchestrator" && strings.HasPrefix(ow.APIVersion, "orchestration.operator.io/") {
			if err := r.Get(ctx, client.ObjectKey{Namespace: pd.Namespace, Name: ow.Name}, &orch); err == nil {
				foundOwner = true
				break
			}
		}
	}
	if !foundOwner {
		log.Info("no owner orchestrator; waiting")
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// 후보 클러스터 산출 (예시: 멤버 전부 혹은 AllowedClusters 우선)
	candidates := orch.Spec.Placement.AllowedClusters
	if len(candidates) == 0 {
		candidates = []string{"member1", "member2"} // 환경에 맞게
	}
	log.Info("scoring start", "promURL", r.PromURL, "candidates", candidates)

	// 점수 계산 (프로메테우스 연동 부분은 네가 넣은 코드 유지)
	scores := make(map[string]int32, len(candidates))
	for _, c := range candidates {
		scores[c] = 1000 // 예: 더미 점수. 실제는 Prometheus로부터 계산
	}
	log.Info("scores (weighted)", "scores", scores)
	// 이전 선택
	prev := ""
	if len(pd.Status.Selected) > 0 {
		prev = pd.Status.Selected[0].Cluster
	}

	// 동점 사전순, 그리고 히스테리시스(차이 < 50면 이전 유지)
	sort.Strings(candidates)
	best := candidates[0]
	for _, c := range candidates {
		if scores[c] > scores[best] {
			best = c
		}
	}
	if prev != "" && best != prev {
		if scores[best] < scores[prev]+50 {
			best = prev
		}
	}

	// Status 업데이트 (불필요 변경 방지)
	newSelected := []orchestrationv1alpha1.SelectedCluster{{Cluster: best}}
	sameSel := (len(pd.Status.Selected) == 1 && pd.Status.Selected[0].Cluster == best)
	if sameSel {
		log.V(1).Info("stable selection", "selected", best)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	now := metav1.Now()
	pd.Status.Selected = newSelected
	pd.Status.Scores = []orchestrationv1alpha1.ClusterScore{
		{Cluster: best, CPU: 1000, Mem: 1000, GPU: 1000, Lat: 1000, Final: scores[best]},
	}
	pd.Status.Reason = "stable selection"
	pd.Status.Updated = &now

	if err := r.Status().Update(ctx, &pd); err != nil {
		return ctrl.Result{}, err
	}
	log.Info("PD selected", "selected", best)
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PlacementDecisionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.PromURL == "" {
		if v := os.Getenv("PROM_URL"); v != "" {
			r.PromURL = v
		}
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&orchestrationv1alpha1.PlacementDecision{}).
		Named("placementdecision").
		Complete(r)
}
