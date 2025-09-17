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

// ──────────────────────────────────────────────────────────────
// Reconciler
type PlacementDecisionReconciler struct {
	client.Client
	Scheme  *runtime.Scheme
	PromURL string
}

// +kubebuilder:rbac:groups=orchestration.operator.io,resources=placementdecisions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=orchestration.operator.io,resources=placementdecisions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=orchestration.operator.io,resources=placementdecisions/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups=orchestration.operator.io,resources=orchestrators,verbs=get;list;watch

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
	httpClient := &http.Client{Timeout: envDuration("PD_PROM_HTTP_TIMEOUT", 3*time.Second)}
	resp, err := httpClient.Do(req)
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

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
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

// candidates: 우선순위 1) 도메인맵 키 2) spec.AllowedClusters 3) 기본값
func deriveCandidates(ctx context.Context, c client.Client, orch *orchestrationv1alpha1.Orchestrator) []string {
	// try domain map
	if domap, err := loadDomainMap(ctx, c, orch.Spec.Namespace); err == nil && len(domap) > 0 {
		out := make([]string, 0, len(domap))
		for k := range domap {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	// fallback: spec
	allowed := append([]string{}, orch.Spec.Placement.AllowedClusters...)
	if len(allowed) == 0 {
		allowed = []string{"member1", "member2"}
	}
	denied := map[string]struct{}{}
	for _, d := range orch.Spec.Placement.DeniedClusters {
		denied[d] = struct{}{}
	}
	out := make([]string, 0, len(allowed))
	for _, cName := range allowed {
		if _, bad := denied[cName]; !bad {
			out = append(out, cName)
		}
	}
	sort.Strings(out)
	return out
}

// weights
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

func loadDomainMap(ctx context.Context, c client.Client, ns string) (map[string]clusterEndpoint, error) {
	cm := &corev1.ConfigMap{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: "orchestrator-domain-map"}, cm); err != nil {
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
// Reconcile
func (r *PlacementDecisionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := crlog.FromContext(ctx)
	log.Info("PD reconcile start", "namespacedName", req.NamespacedName)

	requeueWhenStable := envDuration("PD_REQUEUE_STABLE", 30*time.Second)
	requeueWhenPending := envDuration("PD_REQUEUE_PENDING", 10*time.Second)
	hysteresisMargin := envInt32("PD_HYSTERESIS_MARGIN", defaultHysteresisMargin)
	stickiness := envDuration("PD_STICKINESS", defaultStickiness)
	maxClusters := envInt32("PD_MAX_CLUSTERS", 2)
	multiMargin := envInt32("PD_MULTI_MARGIN", 80)

	// 1) PD 로드
	var pd orchestrationv1alpha1.PlacementDecision
	if err := r.Get(ctx, req.NamespacedName, &pd); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !pd.ObjectMeta.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// (옵션) 강제 재조정 플래그 (nil-safe)
	forceKey := "orchestrator.operator.io/force-reconcile"
	forced := false
	if pd.Annotations != nil {
		forced = strings.TrimSpace(pd.Annotations[forceKey]) != ""
	}

	// 2) Owner Orchestrator
	orch, ok := loadOwnerOrchestrator(ctx, r.Client, &pd)
	if !ok {
		log.Info("no owner orchestrator; waiting owner")
		return ctrl.Result{RequeueAfter: requeueWhenPending}, nil
	}

	// 3) 후보 클러스터
	candidates := deriveCandidates(ctx, r.Client, orch)
	if len(candidates) == 0 {
		log.Info("no candidates after filtering; waiting")
		return ctrl.Result{RequeueAfter: requeueWhenPending}, nil
	}

	// 4) Prom URL
	if r.PromURL == "" {
		if v := os.Getenv("PROM_URL"); v != "" {
			r.PromURL = v
		}
	}
	if r.PromURL == "" {
		log.Info("PROM_URL not set; using neutral scoring (0.5 for all metrics)")
	}

	// 5) 쿼리/가중치
	queries := loadQueries(ctx, r.Client, pd.Namespace)
	wCPU, wMem, wGPU, wLat := weightsFromSpec(orch)

	// 6) 점수계산
	raws := make(map[string]rawScore, len(candidates))
	for _, cName := range candidates {
		cpuVal, memVal, gpuVal, latVal := 0.5, 0.5, 0.5, 0.5
		if queries.cpu != "" {
			q := fmt.Sprintf(queries.cpu, cName)
			if strings.Count(queries.cpu, "%s") == 2 {
				q = fmt.Sprintf(queries.cpu, cName, cName)
			}
			if v := queryInstant(ctx, r.PromURL, q); !math.IsNaN(v) {
				cpuVal = v
			}
		}
		if queries.mem != "" {
			q := fmt.Sprintf(queries.mem, cName)
			if strings.Count(queries.mem, "%s") == 2 {
				q = fmt.Sprintf(queries.mem, cName, cName)
			}
			if v := queryInstant(ctx, r.PromURL, q); !math.IsNaN(v) {
				memVal = v
			}
		}
		if queries.gpu != "" {
			q := fmt.Sprintf(queries.gpu, cName)
			if v := queryInstant(ctx, r.PromURL, q); !math.IsNaN(v) {
				gpuVal = v
			}
		}
		if queries.lat != "" {
			q := fmt.Sprintf(queries.lat, cName)
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

		raws[cName] = rawScore{CPU: cpuVal, Mem: memVal, GPU: gpuVal, Lat: latVal, Final: final}
	}

	// 7) primary(best) + multi-selection with hysteresis/stickiness
	prevPrimary := ""
	var prevAt *metav1.Time
	if len(pd.Status.Selected) > 0 {
		prevPrimary = pd.Status.Selected[0].Cluster
		prevAt = pd.Status.Updated
	}

	// sort by score desc
	sort.SliceStable(candidates, func(i, j int) bool { return raws[candidates[i]].Final > raws[candidates[j]].Final })

	primary := candidates[0]
	if prevPrimary != "" {
		if primary != prevPrimary {
			// keep previous primary if the difference is within hysteresis or stickiness window not elapsed
			if raws[primary].Final < raws[prevPrimary].Final+hysteresisMargin {
				primary = prevPrimary
			}
			if prevAt != nil && time.Since(prevAt.Time) < stickiness {
				primary = prevPrimary
			}
		}
	}

	// Build desired selection list
	desired := make([]orchestrationv1alpha1.SelectedCluster, 0, max(1, int(maxClusters)))
	desired = append(desired, orchestrationv1alpha1.SelectedCluster{Cluster: primary})

	// Keep previously selected (besides primary) if still reasonable and capacity allows
	prevSet := make(map[string]struct{}, len(pd.Status.Selected))
	for _, s := range pd.Status.Selected {
		prevSet[s.Cluster] = struct{}{}
	}
	// preserve order: previously selected (excluding primary) that are still within margin
	for _, s := range pd.Status.Selected {
		if s.Cluster == primary {
			continue
		}
		if raws[s.Cluster].Final >= raws[primary].Final-multiMargin && len(desired) < int(maxClusters) {
			// avoid duplicates
			exists := false
			for _, d := range desired {
				if d.Cluster == s.Cluster {
					exists = true
					break
				}
			}
			if !exists {
				desired = append(desired, orchestrationv1alpha1.SelectedCluster{Cluster: s.Cluster})
			}
		}
	}
	// fill with new candidates within margin until maxClusters
	for _, cName := range candidates {
		if cName == primary {
			continue
		}
		if len(desired) >= int(maxClusters) {
			break
		}
		if raws[cName].Final >= raws[primary].Final-multiMargin {
			exists := false
			for _, d := range desired {
				if d.Cluster == cName {
					exists = true
					break
				}
			}
			if !exists {
				desired = append(desired, orchestrationv1alpha1.SelectedCluster{Cluster: cName})
			}
		}
	}

	// helper to compare selections
	sameSel := func(a, b []orchestrationv1alpha1.SelectedCluster) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i].Cluster != b[i].Cluster {
				return false
			}
		}
		return true
	}(pd.Status.Selected, desired)

	// 강제 재조정이면 주석 제거(1회성) 및 진행
	if forced {
		if pd.Annotations != nil {
			delete(pd.Annotations, forceKey)
			_ = r.Update(ctx, &pd)
		}
	}

	// stable selection: if same selection and not forced, requeue after stable interval
	if sameSel && !forced {
		log.V(1).Info("stable selection",
			"selected", primary, "final", raws[primary].Final)
		return ctrl.Result{RequeueAfter: requeueWhenStable}, nil
	}

	// 10) Status 갱신
	now := metav1.Now()
	toScores := func() []orchestrationv1alpha1.ClusterScore {
		out := make([]orchestrationv1alpha1.ClusterScore, 0, len(candidates))
		for _, cName := range candidates {
			rv := raws[cName]
			out = append(out, orchestrationv1alpha1.ClusterScore{
				Cluster: cName,
				CPU:     int32(math.Round(rv.CPU * 1000)),
				Mem:     int32(math.Round(rv.Mem * 1000)),
				GPU:     int32(math.Round(rv.GPU * 1000)),
				Final:   rv.Final,
			})
		}
		return out
	}()

	pd.Status.Selected = desired
	pd.Status.Scores = toScores
	// reason string with primary and selection set
	selNames := make([]string, 0, len(desired))
	for _, s := range desired {
		selNames = append(selNames, s.Cluster)
	}
	pd.Status.Reason = fmt.Sprintf("primary=%s selected=[%s]", primary, strings.Join(selNames, ","))

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

	if err := r.Status().Update(ctx, &pd); err != nil {
		return ctrl.Result{}, err
	}
	log.Info("PD updated",
		"selected", primary,
		"final", raws[primary].Final,
	)
	return ctrl.Result{RequeueAfter: requeueWhenStable}, nil
}

// ──────────────────────────────────────────────────────────────
// Setup
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

// The selection logic computes both a primary and optional additional clusters for multi-placement.
