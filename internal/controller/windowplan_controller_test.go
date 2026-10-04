package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	api "github.com/iampengqian/windowflow-operator/api/v1alpha1"
	"github.com/iampengqian/windowflow-operator/internal/worker"
	batchv1 "k8s.io/api/batch/v1"
	coordv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, fn := range []func(*runtime.Scheme) error{api.AddToScheme, corev1.AddToScheme, batchv1.AddToScheme, coordv1.AddToScheme} {
		if err := fn(s); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func testPlan() *api.WindowPlan {
	return &api.WindowPlan{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", UID: types.UID("test-plan-uid"), Generation: 1}, Spec: api.WindowPlanSpec{
		PVCName: "cache", SourcePVC: "source", Backend: "local", WorkerImage: "test:local", Slots: 2, CapacityBytes: 14, Readers: []string{"dp0", "dp1"},
		Windows: []api.WindowSpec{{ID: "first", Source: "windows/0000", ExpectedBytes: 6}, {ID: "second", Source: "windows/0001", ExpectedBytes: 6}, {ID: "third", Source: "windows/0002", ExpectedBytes: 8}},
	}}
}

type harness struct {
	t             *testing.T
	c             client.Client
	scheme        *runtime.Scheme
	cache, source string
	key           types.NamespacedName
}

func newHarness(t *testing.T, p *api.WindowPlan) *harness {
	t.Helper()
	scheme := testScheme(t)
	h := &harness{t: t, scheme: scheme, c: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&api.WindowPlan{}, &batchv1.Job{}).WithObjects(p).Build(), cache: t.TempDir(), source: t.TempDir(), key: client.ObjectKeyFromObject(p)}
	for i, content := range []string{"alpha\n", "bravo\n", "charlie\n"} {
		dir := filepath.Join(h.source, fmt.Sprintf("windows/%04d", i))
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "clip.txt"), []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}
	return h
}
func (h *harness) plan() *api.WindowPlan {
	h.t.Helper()
	p := &api.WindowPlan{}
	if err := h.c.Get(context.Background(), h.key, p); err != nil {
		h.t.Fatal(err)
	}
	return p
}
func (h *harness) tick(runWorkers bool) {
	h.t.Helper()
	ctx := context.Background()
	// A fresh reconciler on every tick models loss of all controller memory.
	r := &WindowPlanReconciler{Client: h.c, Scheme: h.scheme}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: h.key}); err != nil {
		h.t.Fatal(err)
	}
	if !runWorkers {
		return
	}
	var jobs batchv1.JobList
	if err := h.c.List(ctx, &jobs); err != nil {
		h.t.Fatal(err)
	}
	for i := range jobs.Items {
		job := &jobs.Items[i]
		if jobComplete(job) {
			continue
		}
		if failed, _ := jobFailed(job); failed {
			continue
		}
		var cfg worker.Config
		if err := json.Unmarshal([]byte(job.Spec.Template.Spec.Containers[0].Env[0].Value), &cfg); err != nil {
			h.t.Fatal(err)
		}
		if err := worker.Run(ctx, cfg, h.cache, h.source); err != nil {
			h.t.Fatalf("%s: %v", job.Name, err)
		}
		now := metav1.Now()
		job.Status.StartTime, job.Status.CompletionTime = &now, &now
		job.Status.Succeeded = 1
		job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue}, {Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
		if err := h.c.Status().Update(ctx, job); err != nil {
			h.t.Fatal(err)
		}
	}
}
func (h *harness) until(check func(*api.WindowPlan) bool) {
	h.t.Helper()
	for range 80 {
		if check(h.plan()) {
			return
		}
		h.tick(true)
	}
	h.t.Fatalf("condition not met: %+v", h.plan().Status)
}
func ready(p *api.WindowPlan, i int32) bool {
	for _, s := range p.Status.Slots {
		if s.WindowIndex == i && s.Phase == "Ready" {
			return true
		}
	}
	return false
}
func (h *harness) release(reader string, i int32, uid string) {
	h.t.Helper()
	p := h.plan()
	if uid == "" {
		uid = string(p.UID)
	}
	gen := fmt.Sprintf("w%06d", i)
	l := &api.WindowLease{ObjectMeta: metav1.ObjectMeta{Name: LeaseName(uid, reader, i, gen), Namespace: p.Namespace}, Spec: api.WindowLeaseSpec{PlanName: p.Name, PlanUID: uid, ReaderID: reader, WindowIndex: i, Generation: gen, Released: true}}
	if err := h.c.Create(context.Background(), l); err != nil {
		h.t.Fatal(err)
	}
}

// Real local filesystem I/O + fake Kubernetes API, including restarts at every
// transition. The separate integration test validates the CRDs on an API server.
func TestTwoSlotsThreeWindowsAllReadersAndRestarts(t *testing.T) {
	h := newHarness(t, testPlan())
	h.until(func(p *api.WindowPlan) bool { return ready(p, 0) && ready(p, 1) })
	if h.plan().Status.NextWindow != 2 {
		t.Fatal("over-allocated slots")
	}
	h.release("dp0", 0, "")
	h.release("dp1", 0, "stale-plan-uid")
	for range 8 {
		h.tick(true)
	}
	if !ready(h.plan(), 0) {
		t.Fatal("missing reader or stale release authorized cleanup")
	}
	old := filepath.Join(h.cache, "windowflow/test-plan-uid/w000000/clip.txt")
	if b, err := os.ReadFile(old); err != nil || string(b) != "alpha\n" {
		t.Fatal("held data changed", err)
	}
	h.release("dp1", 0, "")
	h.until(func(p *api.WindowPlan) bool { return ready(p, 2) })
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatal("old generation not cleaned")
	}
	p := h.plan()
	if len(p.Status.Slots) != 2 || p.Status.CompletedWindows != 1 {
		t.Fatalf("bad slot reuse: %+v", p.Status)
	}
	for _, i := range []int32{1, 2} {
		h.release("dp0", i, "")
		h.release("dp1", i, "")
	}
	h.until(func(p *api.WindowPlan) bool { return p.Status.Phase == "Completed" })
	h.tick(true)
	var lock coordv1.Lease
	if err := h.c.Get(context.Background(), types.NamespacedName{Name: lockName("cache"), Namespace: "default"}, &lock); !apierrors.IsNotFound(err) {
		t.Fatal("completed plan retained PVC lock", err)
	}
	if b, err := os.ReadFile(filepath.Join(h.source, "windows/0000/clip.txt")); err != nil || string(b) != "alpha\n" {
		t.Fatal("source changed", err)
	}
}

func TestCapacityAndConcurrentLoadsBoundAllocation(t *testing.T) {
	p := testPlan()
	p.Spec.Slots = 4
	p.Spec.MaxConcurrentLoads = 2
	p.Spec.CapacityBytes = 12
	h := newHarness(t, p)
	for range 8 {
		h.tick(false)
	}
	if got := h.plan().Status.NextWindow; got != 2 {
		t.Fatalf("expected 2 reservations, got %d", got)
	}
	h.until(func(p *api.WindowPlan) bool { return ready(p, 0) && ready(p, 1) })
	for range 8 {
		h.tick(true)
	}
	if h.plan().Status.NextWindow != 2 {
		t.Fatal("byte budget ignored with free slots")
	}
}

func TestPVCExclusionSurvivesPlanDeletion(t *testing.T) {
	h := newHarness(t, testPlan())
	h.tick(false)
	old := h.plan()
	other := testPlan()
	other.Name = "other"
	other.UID = "other-uid"
	if err := h.c.Create(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	h.key = client.ObjectKeyFromObject(other)
	h.tick(false)
	if h.plan().Status.Phase != "Pending" {
		t.Fatal("second plan acquired PVC")
	}
	if err := h.c.Delete(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	h.tick(false)
	if h.plan().Status.Phase != "Pending" {
		t.Fatal("deletion incorrectly freed a possibly active PVC")
	}
}

func TestFailedWorkerFreezesReservations(t *testing.T) {
	h := newHarness(t, testPlan())
	for range 3 {
		h.tick(false)
	}
	var jobs batchv1.JobList
	if err := h.c.List(context.Background(), &jobs); err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) == 0 {
		t.Fatal("no worker created")
	}
	j := &jobs.Items[0]
	j.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "ReadFailed"}}
	if err := h.c.Status().Update(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	h.tick(false)
	before := h.plan().Status.NextWindow
	for range 8 {
		h.tick(false)
	}
	p := h.plan()
	if p.Status.Phase != "Failed" || len(p.Status.Slots) == 0 || p.Status.NextWindow != before {
		t.Fatalf("failure did not freeze: %+v", p.Status)
	}
}

func TestResumeStagesOnlyCheckpointSuffix(t *testing.T) {
	p := testPlan()
	p.Spec.StartWindow = 2
	h := newHarness(t, p)
	h.until(func(p *api.WindowPlan) bool { return ready(p, 2) })
	if len(h.plan().Status.Slots) != 1 {
		t.Fatal("staged skipped windows")
	}
	h.release("dp0", 2, "")
	h.release("dp1", 2, "")
	h.until(func(p *api.WindowPlan) bool { return p.Status.Phase == "Completed" })
	if h.plan().Status.CompletedWindows != 1 {
		t.Fatal("completion count should count this attempt")
	}
}

func TestInvalidPlanAndWorkerSecurity(t *testing.T) {
	for _, edit := range []func(*api.WindowPlan){func(p *api.WindowPlan) { p.Spec.Readers = []string{"dp0", "dp0"} }, func(p *api.WindowPlan) { p.Spec.Windows[0].Source = "../escape" }, func(p *api.WindowPlan) { p.Spec.StartWindow = 3 }, func(p *api.WindowPlan) { p.Spec.Slots = 1 }, func(p *api.WindowPlan) { p.Spec.SourcePVC = p.Spec.PVCName }} {
		p := testPlan()
		edit(p)
		if ValidatePlan(p) == nil {
			t.Fatal("invalid plan accepted")
		}
	}
	p := testPlan()
	slot := &api.SlotStatus{StageJob: "stage", CleanJob: "clean", WindowIndex: 0, Generation: "w000000", RelativePath: "windowflow/test-plan-uid/w000000", ReservedBytes: 6}
	j, err := makeJob(p, slot, "stage")
	if err != nil {
		t.Fatal(err)
	}
	if *j.Spec.Template.Spec.AutomountServiceAccountToken || !j.Spec.Template.Spec.Containers[0].VolumeMounts[1].ReadOnly || *j.Spec.BackoffLimit != 0 {
		t.Fatal("unsafe worker defaults")
	}
	p.Spec.Backend = "cpfs-dataflow"
	p.Spec.DataFlow = &api.DataFlowSpec{CredentialsSecret: "cloud"}
	j, err = makeJob(p, slot, "clean")
	if err != nil {
		t.Fatal(err)
	}
	if len(j.Spec.Template.Spec.Containers[0].Env) != 1 {
		t.Fatal("cleanup must not receive cloud credentials")
	}
}
