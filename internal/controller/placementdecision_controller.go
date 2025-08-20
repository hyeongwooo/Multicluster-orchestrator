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
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
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
// Prometheus API response (instant query)
// Prometheus API response (instant query)
type promAPIResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  []any             `json:"value"` // [ <unix_ts>, "<string_value>" ]
		} `json:"result"`
	} `json:"data"`
}

const (
	defaultHysteresisMargin int32         = 50               // 점수차 50 미만이면 이전 유지
	defaultStickiness       time.Duration = 90 * time.Second // prefer previous for short window
)

// Helper functions for env-driven knobs
func envDuration(key string, def time.Duration) time.Duration {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func envInt(key string, def int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}

func envInt32(key string, def int32) int32 {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return int32(i)
		}
	}
	return def
}

// read query result as float64 (returns NaN if missing)
func queryInstant(ctx context.Context, baseURL, q string) float64 {
	if baseURL == "" || q == "" {
		return math.NaN()
	}
	qq := url.Values{}
	qq.Set("query", q)
	u := strings.TrimRight(baseURL, "/") + "/api/v1/query?" + qq.Encode()

	req, _ := http.NewRequestWithContext(ctx, "GET", u, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return math.NaN()
	}
	defer resp.Body.Close()

	var pr promAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return math.NaN()
	}
	if pr.Status != "success" || len(pr.Data.Result) == 0 || len(pr.Data.Result[0].Value) < 2 {
		return math.NaN()
	}
	sv, _ := pr.Data.Result[0].Value[1].(string)
	f, err := strconvParse(sv)
	if err != nil {
		return math.NaN()
	}
	return f
}

func strconvParse(s string) (float64, error) {
	return strconvParseFloat(s)
}

func strconvParseFloat(s string) (float64, error) {
	return strconv.ParseFloat(strings.TrimSpace(s), 64)
}

func (r *PlacementDecisionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := crlog.FromContext(ctx)
	log.Info("PD reconcile start", "namespacedName", req.NamespacedName)

	// Requeue / policy knobs from env (optional overrides)
	requeueWhenStable := envDuration("PD_REQUEUE_STABLE", 30*time.Second)
	requeueWhenPending := envDuration("PD_REQUEUE_PENDING", 10*time.Second)
	hysteresisMargin := envInt32("PD_HYSTERESIS_MARGIN", defaultHysteresisMargin)
	stickiness := envDuration("PD_STICKINESS", defaultStickiness)

	// 1) PD 로드
	var pd orchestrationv1alpha1.PlacementDecision
	if err := r.Get(ctx, req.NamespacedName, &pd); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !pd.ObjectMeta.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// 2) Owner Orchestrator 찾기
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
		log.Info("no owner orchestrator; waiting owner")
		return ctrl.Result{RequeueAfter: requeueWhenPending}, nil
	}

	// 3) 후보 클러스터
	candidates := orch.Spec.Placement.AllowedClusters
	if len(candidates) == 0 {
		candidates = []string{"member1", "member2"}
	}
	sort.Strings(candidates)

	// 4) Prom URL 확정 (env 우선)
	if r.PromURL == "" {
		if v := os.Getenv("PROM_URL"); v != "" {
			r.PromURL = v
		}
	}
	log.Info("scoring start", "promURL", r.PromURL, "candidates", candidates)

	// 5) 쿼리 템플릿 로드 (옵션)
	type qset struct{ cpu, mem, gpu, lat string }
	var queries qset
	{
		cm := &corev1.ConfigMap{}
		if err := r.Get(ctx, client.ObjectKey{Namespace: pd.Namespace, Name: "orchestrator-prom-queries"}, cm); err == nil {
			queries.cpu = strings.TrimSpace(cm.Data["cpu_query"])
			queries.mem = strings.TrimSpace(cm.Data["mem_query"])
			queries.gpu = strings.TrimSpace(cm.Data["gpu_query"])
			queries.lat = strings.TrimSpace(cm.Data["latency_query"])
		}
	}
	// Built-in query defaults when CM keys are empty
	if strings.TrimSpace(queries.cpu) == "" {
		queries.cpu = `avg(rate(node_cpu_seconds_total{mode!="idle", cluster="%s"}[2m]))`
	}
	if strings.TrimSpace(queries.mem) == "" {
		queries.mem = `(1 - (node_memory_MemAvailable_bytes{cluster="%s"} / node_memory_MemTotal_bytes{cluster="%s"}))`
	}
	if strings.TrimSpace(queries.gpu) == "" {
		queries.gpu = `avg(nvidia_gpu_utilization{cluster="%s"})`
	}
	if strings.TrimSpace(queries.lat) == "" {
		queries.lat = `histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{cluster="%s"}[5m])) by (le))`
	}
	queriesDump := map[string]string{
		"cpu_query":     queries.cpu,
		"mem_query":     queries.mem,
		"gpu_query":     queries.gpu,
		"latency_query": queries.lat,
	}

	// 6) 가중치 (없으면 1.0씩)
	wCPU, wMem, wGPU, wLat := 1, 1, 1, 1

	// 7) 점수 계산
	type raw struct {
		CPU, Mem, GPU, Lat float64
		Final              int32
	}
	raws := make(map[string]raw, len(candidates))

	for _, c := range candidates {
		var (
			cpuVal = 1.0
			memVal = 1.0
			gpuVal = 1.0
			latVal = 1.0
		)
		// Prometheus 조회 (있으면 사용)
		if queries.cpu != "" {
			q := fmt.Sprintf(queries.cpu, c)
			if strings.Count(queries.cpu, "%s") == 2 {
				q = fmt.Sprintf(queries.cpu, c, c)
			}
			if v := queryInstant(ctx, r.PromURL, q); !math.IsNaN(v) {
				cpuVal = v
			}
		}
		if queries.mem != "" {
			q := fmt.Sprintf(queries.mem, c)
			if strings.Count(queries.mem, "%s") == 2 {
				q = fmt.Sprintf(queries.mem, c, c)
			}
			if v := queryInstant(ctx, r.PromURL, q); !math.IsNaN(v) {
				memVal = v
			}
		}
		if queries.gpu != "" {
			q := fmt.Sprintf(queries.gpu, c)
			if v := queryInstant(ctx, r.PromURL, q); !math.IsNaN(v) {
				gpuVal = v
			}
		}
		if queries.lat != "" {
			q := fmt.Sprintf(queries.lat, c)
			if v := queryInstant(ctx, r.PromURL, q); !math.IsNaN(v) {
				latVal = v
			}
		}

		// 간단 정규화
		norm := func(x float64) float64 {
			if math.IsNaN(x) || math.IsInf(x, 0) {
				return 0.5
			}
			if x < 0 {
				return 0
			}
			if x > 1 {
				return 1
			}
			return x
		}
		scoreCPU := 1 - norm(cpuVal) // 사용률 낮을수록 좋음
		scoreMem := 1 - norm(memVal)
		scoreGPU := 1 - norm(gpuVal)
		scoreLat := 1 / (1 + latVal) // 지연 낮을수록 좋음

		// ==== 여기 수정: wt 대신 wCPU/wMem/wGPU/wLat 사용 ====
		num := float64(wCPU)*scoreCPU + float64(wMem)*scoreMem + float64(wGPU)*scoreGPU + float64(wLat)*scoreLat
		den := float64(wCPU + wMem + wGPU + wLat)
		final := int32(math.Round(1000.0 * num / den))

		raws[c] = raw{CPU: cpuVal, Mem: memVal, GPU: gpuVal, Lat: latVal, Final: final}
	}

	// 8) 베스트 선택 + 히스테리시스/스티키니스
	prev := ""
	var prevAt *metav1.Time
	if len(pd.Status.Selected) > 0 {
		prev = pd.Status.Selected[0].Cluster
		prevAt = pd.Status.Updated
	}
	best := candidates[0]
	for _, c := range candidates {
		if raws[c].Final > raws[best].Final {
			best = c
		}
	}
	margin := hysteresisMargin
	if prev != "" && best != prev {
		if raws[best].Final < raws[prev].Final+margin {
			best = prev
		}
		if prevAt != nil && time.Since(prevAt.Time) < stickiness {
			best = prev
		}
	}

	// 9) Status 채우기
	toScores := func() []orchestrationv1alpha1.ClusterScore {
		out := make([]orchestrationv1alpha1.ClusterScore, 0, len(candidates))
		for _, c := range candidates {
			rv := raws[c]
			out = append(out, orchestrationv1alpha1.ClusterScore{
				Cluster: c,
				CPU:     int32(math.Round(rv.CPU * 1000)),
				Mem:     int32(math.Round(rv.Mem * 1000)),
				GPU:     int32(math.Round(rv.GPU * 1000)),
				Lat:     int32(math.Round(rv.Lat * 1000)),
				Final:   rv.Final,
			})
		}
		return out
	}()

	newSelected := []orchestrationv1alpha1.SelectedCluster{{Cluster: best}}
	sameSel := (len(pd.Status.Selected) == 1 && pd.Status.Selected[0].Cluster == best)

	// Force reconcile annotation trigger
	forceKey := "orchestration.operator.io/force-reconcile"
	forceRequested := strings.TrimSpace(pd.Annotations[forceKey]) != ""

	if sameSel && !forceRequested {
		log.V(1).Info("stable selection", "selected", best, "final", raws[best].Final)
		return ctrl.Result{RequeueAfter: requeueWhenStable}, nil
	}

	// If force reconcile requested, remove the annotation to avoid busy-looping
	if forceRequested {
		if pd.Annotations == nil {
			pd.Annotations = map[string]string{}
		}
		delete(pd.Annotations, forceKey)
		if err := r.Update(ctx, &pd); err != nil {
			log.Error(err, "failed to remove force-reconcile annotation")
		}
	}

	now := metav1.Now()
	pd.Status.Selected = newSelected
	pd.Status.Scores = toScores
	pd.Status.Reason = fmt.Sprintf("best=%s final=%d", best, raws[best].Final)
	pd.Status.Updated = &now
	// PlacementDebug 타입이 CRD에 정의되어 있어야 합니다.
	pd.Status.Debug = &orchestrationv1alpha1.PlacementDebug{
		PromURL: r.PromURL,
		Queries: queriesDump,
	}

	if err := r.Status().Update(ctx, &pd); err != nil {
		return ctrl.Result{}, err
	}
	log.Info("PD selected", "selected", best, "final", raws[best].Final)
	return ctrl.Result{RequeueAfter: requeueWhenStable}, nil
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
