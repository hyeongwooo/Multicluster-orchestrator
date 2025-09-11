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
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch

// ──────────────────────────────────────────────────────────────
// Prometheus API response
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
	defaultHysteresisMargin int32         = 50
	defaultStickiness       time.Duration = 90 * time.Second

	// Health check defaults (for NATS monitoring endpoint)
	defaultMonPath    = "/healthz"
	defaultHTTPTO     = 2 * time.Second
	defaultUseCluster = true
)

// ──────────────────────────────────────────────────────────────
// env knobs
func envDuration(key string, def time.Duration) time.Duration {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
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

func envBool(key string, def bool) bool {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		switch strings.ToLower(v) {
		case "1", "t", "true", "y", "yes", "on":
			return true
		case "0", "f", "false", "n", "no", "off":
			return false
		}
	}
	return def
}

// ──────────────────────────────────────────────────────────────
// prometheus instant query helpers
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
	f, err := strconv.ParseFloat(strings.TrimSpace(sv), 64)
	if err != nil {
		return math.NaN()
	}
	return f
}

// ──────────────────────────────────────────────────────────────
// scoring helpers
type qset struct{ cpu, mem, gpu, lat string }

func loadQueries(ctx context.Context, c client.Client, ns string) qset {
	qs := qset{}
	cm := &corev1.ConfigMap{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: "orchestrator-prom-queries"}, cm); err == nil {
		qs.cpu = strings.TrimSpace(cm.Data["cpu_query"])
		qs.mem = strings.TrimSpace(cm.Data["mem_query"])
		qs.gpu = strings.TrimSpace(cm.Data["gpu_query"])
		qs.lat = strings.TrimSpace(cm.Data["latency_query"])
	}
	// defaults
	if qs.cpu == "" {
		qs.cpu = `avg(rate(node_cpu_seconds_total{mode!="idle", cluster="%s"}[2m]))`
	}
	if qs.mem == "" {
		qs.mem = `(1 - (node_memory_MemAvailable_bytes{cluster="%s"} / node_memory_MemTotal_bytes{cluster="%s"}))`
	}
	if qs.gpu == "" {
		qs.gpu = `avg(nvidia_gpu_utilization{cluster="%s"})`
	}
	if qs.lat == "" {
		qs.lat = `histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{cluster="%s"}[5m])) by (le))`
	}
	return qs
}

type rawScore struct {
	CPU, Mem, GPU, Lat float64
	Final              int32
}

func norm01(x float64) float64 {
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

// ──────────────────────────────────────────────────────────────
// owner Orchestrator helpers
func loadOwnerOrchestrator(ctx context.Context, c client.Client, pd *orchestrationv1alpha1.PlacementDecision) (*orchestrationv1alpha1.Orchestrator, bool) {
	var orch orchestrationv1alpha1.Orchestrator
	for _, ow := range pd.OwnerReferences {
		if ow.Kind == "Orchestrator" && strings.HasPrefix(ow.APIVersion, "orchestration.operator.io/") {
			if err := c.Get(ctx, client.ObjectKey{Namespace: pd.Namespace, Name: ow.Name}, &orch); err == nil {
				return &orch, true
			}
		}
	}
	return nil, false
}

// candidates from spec + allowed/denied
func deriveCandidates(orch *orchestrationv1alpha1.Orchestrator) []string {
	allowed := append([]string{}, orch.Spec.Placement.AllowedClusters...)
	if len(allowed) == 0 {
		allowed = []string{"member1", "member2"}
	}
	denied := map[string]struct{}{}
	for _, d := range orch.Spec.Placement.DeniedClusters {
		denied[d] = struct{}{}
	}
	out := make([]string, 0, len(allowed))
	for _, c := range allowed {
		if _, bad := denied[c]; !bad {
			out = append(out, c)
		}
	}
	sort.Strings(out)
	return out
}

func weightsFromSpec(orch *orchestrationv1alpha1.Orchestrator) (wCPU, wMem, wGPU, wLat int) {
	get := func(p *int32, def int) int {
		if p == nil {
			return def
		}
		if *p <= 0 {
			return 0
		}
		return int(*p)
	}
	wCPU = get(orch.Spec.Placement.CPUWeight, 1)
	wMem = get(orch.Spec.Placement.MemWeight, 1)
	wGPU = get(orch.Spec.Placement.GPUWeight, 1)
	wLat = get(orch.Spec.Placement.LatencyWeight, 1)
	if wCPU+wMem+wGPU+wLat == 0 {
		return 1, 1, 1, 1
	}
	return
}

// EB home policy: annotation → env → best(eswt)
func decideEBHome(pd *orchestrationv1alpha1.PlacementDecision, best string) string {
	if v := strings.TrimSpace(pd.Annotations["orchestrator.operator.io/eb-home"]); v != "" {
		return v
	}
	if v := strings.TrimSpace(os.Getenv("EB_HOME")); v != "" {
		return v
	}
	return best
}

// buildBusURLs returns:
// - client scheme (nats/tls)
// - service FQDN (clusterset or cluster local)
// - client BusURL (scheme://host:4222)
// - monitor URL (http://host:8222/healthz) for health check
func buildBusURLs(ns string) (scheme, svcFQDN, clientURL, monitorURL string) {
	scheme = strings.TrimSpace(os.Getenv("PD_BUS_SCHEME"))
	if scheme == "" {
		scheme = "tls"
	}
	useClusterset := envBool("USE_CLUSTERSET_DNS", defaultUseCluster)
	host := fmt.Sprintf("nats-bus.%s.svc.cluster.local", ns)
	if useClusterset {
		host = fmt.Sprintf("nats-bus.%s.svc.clusterset.local", ns)
	}
	clientURL = fmt.Sprintf("%s://%s:%d", scheme, host, 4222)

	monPath := strings.TrimSpace(os.Getenv("PD_BUS_MON_PATH"))
	if monPath == "" {
		monPath = defaultMonPath
	}
	monitorURL = fmt.Sprintf("http://%s:%d%s", host, 8222, monPath)
	svcFQDN = host
	return
}

// Health check to NATS monitor endpoint (8222)
func checkBusHealth(monitorURL string) bool {
	if strings.TrimSpace(monitorURL) == "" {
		return true
	}
	cl := &http.Client{Timeout: envDuration("PD_MON_HTTP_TIMEOUT", defaultHTTPTO)}
	req, _ := http.NewRequest("GET", monitorURL, nil)
	resp, err := cl.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

// ──────────────────────────────────────────────────────────────
// Reconcile
func (r *PlacementDecisionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := crlog.FromContext(ctx)
	log.Info("PD reconcile start", "namespacedName", req.NamespacedName)

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

	// (옵션) 강제 재조정 플래그
	forceKey := "orchestrator.operator.io/force-reconcile"
	forced := strings.TrimSpace(pd.Annotations[forceKey]) != ""

	// 2) Owner Orchestrator
	orch, ok := loadOwnerOrchestrator(ctx, r.Client, &pd)
	if !ok {
		log.Info("no owner orchestrator; waiting owner")
		return ctrl.Result{RequeueAfter: requeueWhenPending}, nil
	}

	// 3) 후보 클러스터
	candidates := deriveCandidates(orch)
	if len(candidates) == 0 {
		log.Info("no candidates after filtering; waiting")
		return ctrl.Result{RequeueAfter: requeueWhenPending}, nil
	}

	// Prom URL
	if r.PromURL == "" {
		if v := os.Getenv("PROM_URL"); v != "" {
			r.PromURL = v
		}
	}
	if r.PromURL == "" {
		log.Info("PROM_URL not set; using neutral scoring (0.5 for all metrics)")
	}
	log.Info("scoring start", "promURL", r.PromURL, "candidates", candidates)

	// 5) 쿼리/가중치
	queries := loadQueries(ctx, r.Client, pd.Namespace)
	wCPU, wMem, wGPU, wLat := weightsFromSpec(orch)

	// 6) 점수계산
	raws := make(map[string]rawScore, len(candidates))
	for _, c := range candidates {
		cpuVal, memVal, gpuVal, latVal := 0.5, 0.5, 0.5, 0.5
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

		scoreCPU := 1 - norm01(cpuVal)
		scoreMem := 1 - norm01(memVal)
		scoreGPU := 1 - norm01(gpuVal)
		scoreLat := 1 / (1 + math.Max(latVal, 0))

		num := float64(wCPU)*scoreCPU + float64(wMem)*scoreMem + float64(wGPU)*scoreGPU + float64(wLat)*scoreLat
		den := float64(wCPU + wMem + wGPU + wLat)
		final := int32(math.Round(1000.0 * num / den))

		raws[c] = rawScore{CPU: cpuVal, Mem: memVal, GPU: gpuVal, Lat: latVal, Final: final}
	}

	// 7) best + 히스테리시스/스티키니스
	prev := ""
	var prevAt *metav1.Time
	if len(pd.Status.Selected) > 0 {
		prev = pd.Status.Selected[0].Cluster
		prevAt = pd.Status.Updated
	}

	// sort candidates by score desc
	sort.SliceStable(candidates, func(i, j int) bool { return raws[candidates[i]].Final > raws[candidates[j]].Final })
	best := candidates[0]
	if prev != "" {
		// hysteresis & stickiness
		if best != prev {
			if raws[best].Final < raws[prev].Final+hysteresisMargin {
				best = prev
			}
			if prevAt != nil && time.Since(prevAt.Time) < stickiness {
				best = prev
			}
		}
	}

	newSelected := []orchestrationv1alpha1.SelectedCluster{{Cluster: best}}
	sameSel := (len(pd.Status.Selected) == 1 && pd.Status.Selected[0].Cluster == best)

	// 8) EB 홈/BusURL/MonitorURL 결정 (PD가 권위자)
	// 기본은 best를 허브로 본다.
	home := decideEBHome(&pd, best)
	_, svcFQDN, busURL, monitorURL := buildBusURLs(pd.Namespace)

	// 허브(=home) 헬스체크 → 불건강하면 다음 후보로 EB 홈 전환
	healthy := checkBusHealth(monitorURL)
	if !healthy {
		if len(candidates) > 1 {
			for _, c := range candidates {
				if c != best {
					home = c
					break
				}
			}
		}
		// clusterset DNS를 쓰면 URL은 동일, 실제 Endpoint만 엣지로 전환됨
	}
	ebHome := home

	// 강제 재조정이면 주석 제거(1회성) 및 진행
	if forced {
		if pd.Annotations == nil {
			pd.Annotations = map[string]string{}
		}
		delete(pd.Annotations, forceKey)
		_ = r.Update(ctx, &pd)
	}

	// 9) stable이면 빠르게 리턴 (단, EventInfra가 바뀌었으면 갱신)
	needsEvtInfraUpdate :=
		pd.Status.EventInfra == nil ||
			pd.Status.EventInfra.EventBusHome != ebHome ||
			pd.Status.EventInfra.BusURL != busURL ||
			pd.Status.EventInfra.GlobalNATSServiceName != "nats-bus"

	if sameSel && !needsEvtInfraUpdate && !forced {
		log.V(1).Info("stable selection",
			"selected", best, "final", raws[best].Final,
			"home", ebHome, "healthy", healthy)
		return ctrl.Result{RequeueAfter: requeueWhenStable}, nil
	}

	// 10) Status 갱신
	now := metav1.Now()
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

	pd.Status.Selected = newSelected
	pd.Status.Scores = toScores
	pd.Status.Reason = fmt.Sprintf("best=%s final=%d healthy=%v", best, raws[best].Final, healthy)
	pd.Status.Updated = &now
	pd.Status.Debug = &orchestrationv1alpha1.PlacementDebug{
		PromURL: r.PromURL,
		Queries: map[string]string{
			"cpu_query":     queries.cpu,
			"mem_query":     queries.mem,
			"gpu_query":     queries.gpu,
			"latency_query": queries.lat,
		},
	}

	// EventInfra 확정 (PD = 권위자)
	pd.Status.EventInfra = &orchestrationv1alpha1.EventInfraStatus{
		EventBusHome:          ebHome,     // 허브 or Failover된 엣지
		GlobalNATSServiceName: "nats-bus", // Orchestrator에서 글로벌 SVC 이름으로 사용
		NatsBackendSelector: map[string]string{
			"controller":    "eventbus-controller",
			"eventbus-name": "default",
		},
		BusURL: busURL, // e.g. tls://nats-bus.<ns>.svc.clusterset.local:4222
	}

	if err := r.Status().Update(ctx, &pd); err != nil {
		return ctrl.Result{}, err
	}
	log.Info("PD updated",
		"selected", best,
		"final", raws[best].Final,
		"ebHome", ebHome,
		"busURL", busURL,
		"monitorURL", monitorURL,
		"globalSvc", svcFQDN,
		"healthy", healthy,
	)
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
