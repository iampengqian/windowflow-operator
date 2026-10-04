//go:build integration

package controller

import (
	"context"
	"testing"

	api "github.com/iampengqian/windowflow-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestRealAPIValidationAndReconciliation(t *testing.T) {
	env := &envtest.Environment{CRDDirectoryPaths: []string{"../../config/crd/bases"}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Error(err)
		}
	})
	c, err := client.New(cfg, client.Options{Scheme: testScheme(t)})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	p := testPlan()
	p.UID = ""
	p.Generation = 0
	if err := c.Create(ctx, p); err != nil {
		t.Fatal(err)
	}
	// API admission must actually enforce the immutable spec, not just Go code.
	changed := p.DeepCopy()
	changed.Spec.CapacityBytes++
	if err := c.Update(ctx, changed); !apierrors.IsInvalid(err) {
		t.Fatal("plan mutation not rejected", err)
	}
	l := &api.WindowLease{ObjectMeta: metav1.ObjectMeta{Name: "validation-lease", Namespace: "default"}, Spec: api.WindowLeaseSpec{PlanName: p.Name, PlanUID: string(p.UID), ReaderID: "dp0", WindowIndex: 0, Generation: "w000000", Released: true}}
	if err := c.Create(ctx, l); err != nil {
		t.Fatal(err)
	}
	l.Spec.Released = false
	if err := c.Update(ctx, l); !apierrors.IsInvalid(err) {
		t.Fatal("release rollback not rejected", err)
	}
	l.Spec.Released = true
	l.Spec.Generation = "w000001"
	if err := c.Update(ctx, l); !apierrors.IsInvalid(err) {
		t.Fatal("lease identity mutation not rejected", err)
	}
	// Use a real API for reservations/Jobs; envtest intentionally has no kubelet
	// or Job controller, so the harness executes workers and reports their result.
	h := newHarness(t, testPlan())
	h.c = c
	h.key = client.ObjectKeyFromObject(p)
	h.until(func(p *api.WindowPlan) bool { return ready(p, 0) && ready(p, 1) })
	for _, i := range []int32{0, 1} {
		h.release("dp0", i, "")
		h.release("dp1", i, "")
	}
	h.until(func(p *api.WindowPlan) bool { return ready(p, 2) })
	h.release("dp0", 2, "")
	h.release("dp1", 2, "")
	h.until(func(p *api.WindowPlan) bool { return p.Status.Phase == "Completed" })
}
