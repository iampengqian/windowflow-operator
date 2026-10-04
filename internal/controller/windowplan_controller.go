// Copyright 2026 WindowFlow contributors. SPDX-License-Identifier: Apache-2.0
package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	coordv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	api "github.com/iampengqian/windowflow-operator/api/v1alpha1"
)

const pollInterval = 2 * time.Second

type WindowPlanReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

func (r *WindowPlanReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&api.WindowPlan{}).Owns(&batchv1.Job{}).
		Watches(&api.WindowLease{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			lease := obj.(*api.WindowLease)
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: lease.Namespace, Name: lease.Spec.PlanName}}}
		})).Complete(r)
}

func (r *WindowPlanReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var plan api.WindowPlan
	if err := r.Get(ctx, req.NamespacedName, &plan); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !plan.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	if plan.Status.Phase == "Completed" {
		return ctrl.Result{}, r.releasePVCLock(ctx, &plan)
	}
	if plan.Status.Phase == "Failed" {
		return ctrl.Result{}, nil
	}
	if err := ValidatePlan(&plan); err != nil {
		return r.fail(ctx, &plan, err.Error())
	}

	owned, err := r.ensurePVCLock(ctx, &plan)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !owned {
		if plan.Status.Phase != "Pending" || plan.Status.Message != "PVC is owned by another plan; ownership is never stolen on timeout" {
			plan.Status.Phase = "Pending"
			plan.Status.Message = "PVC is owned by another plan; ownership is never stolen on timeout"
			if err := r.Status().Update(ctx, &plan); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}
	if plan.Status.Phase != "Running" {
		if plan.Status.ObservedGeneration == 0 {
			plan.Status.NextWindow = plan.Spec.StartWindow
		}
		plan.Status.Phase, plan.Status.Message = "Running", ""
		plan.Status.ObservedGeneration = plan.Generation
		return r.save(ctx, &plan)
	}

	var leases api.WindowLeaseList
	if err := r.List(ctx, &leases, client.InNamespace(plan.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	for i := range plan.Status.Slots {
		slot := &plan.Status.Slots[i]
		if slot.WindowIndex < 0 || int(slot.WindowIndex) >= len(plan.Spec.Windows) {
			return r.fail(ctx, &plan, "invalid persisted window ordinal; refusing side effects")
		}
		switch slot.Phase {
		case "Loading", "Reclaiming":
			action := "stage"
			if slot.Phase == "Reclaiming" {
				action = "clean"
			}
			job, err := r.ensureJob(ctx, &plan, slot, action)
			if err != nil {
				return ctrl.Result{}, err
			}
			if failed, message := jobFailed(job); failed {
				slot.Phase = "Failed"
				return r.fail(ctx, &plan, fmt.Sprintf("%s job %s failed: %s; data retained for inspection", action, job.Name, message))
			}
			if !jobComplete(job) {
				continue
			}
			if action == "stage" {
				slot.Phase = "Ready"
				return r.save(ctx, &plan)
			}
			// Reservations are kept until cleanup succeeds. Completed jobs remain owned by
			// the plan for audit; no running writer is removed to make a slot reusable.
			plan.Status.CompletedWindows++
			plan.Status.Slots = append(plan.Status.Slots[:i], plan.Status.Slots[i+1:]...)
			return r.save(ctx, &plan)
		case "Ready":
			if allReleased(&plan, slot, leases.Items) {
				slot.Phase = "Reclaiming"
				return r.save(ctx, &plan)
			}
		default:
			return r.fail(ctx, &plan, "unexpected persisted slot phase; refusing side effects")
		}
	}

	if int(plan.Status.NextWindow) == len(plan.Spec.Windows) && len(plan.Status.Slots) == 0 {
		plan.Status.Phase = "Completed"
		plan.Status.Message = "All windows explicitly released and cleaned"
		return r.save(ctx, &plan)
	}
	if plan.Status.NextWindow < 0 || int(plan.Status.NextWindow) > len(plan.Spec.Windows) {
		return r.fail(ctx, &plan, "invalid nextWindow")
	}
	if int(plan.Status.NextWindow) < len(plan.Spec.Windows) {
		index, reserved, loading := freeSlot(&plan)
		limit := plan.Spec.MaxConcurrentLoads
		if limit == 0 {
			limit = 1
		}
		window := plan.Spec.Windows[plan.Status.NextWindow]
		if index >= 0 && loading < limit && window.ExpectedBytes <= plan.Spec.CapacityBytes-reserved {
			ordinal := plan.Status.NextWindow
			generation := fmt.Sprintf("w%06d", ordinal)
			prefix := "wf-" + hash(string(plan.UID))[:16] + "-" + generation
			plan.Status.Slots = append(plan.Status.Slots, api.SlotStatus{
				Index: index, WindowIndex: ordinal, WindowID: window.ID, Generation: generation,
				Phase: "Loading", RelativePath: path.Join("windowflow", string(plan.UID), generation),
				ReservedBytes: window.ExpectedBytes, StageJob: prefix + "-stage", CleanJob: prefix + "-clean",
			})
			plan.Status.NextWindow++
			// Persist the reservation and identity before issuing external work.
			return r.save(ctx, &plan)
		}
	}
	return ctrl.Result{RequeueAfter: pollInterval}, nil
}

func (r *WindowPlanReconciler) save(ctx context.Context, plan *api.WindowPlan) (ctrl.Result, error) {
	return ctrl.Result{RequeueAfter: 100 * time.Millisecond}, r.Status().Update(ctx, plan)
}

func (r *WindowPlanReconciler) fail(ctx context.Context, plan *api.WindowPlan, message string) (ctrl.Result, error) {
	plan.Status.Phase, plan.Status.Message = "Failed", message
	return ctrl.Result{}, r.Status().Update(ctx, plan)
}

func ValidatePlan(plan *api.WindowPlan) error {
	s := plan.Spec
	if len(validation.IsDNS1123Subdomain(s.PVCName)) != 0 {
		return fmt.Errorf("invalid pvcName")
	}
	if s.Slots < 2 || s.Slots > 64 || s.CapacityBytes <= 0 {
		return fmt.Errorf("slots must be 2..64 and capacityBytes positive")
	}
	if s.WorkerImage == "" || len(s.Readers) == 0 || len(s.Windows) == 0 {
		return fmt.Errorf("workerImage, readers and windows are required")
	}
	if s.StartWindow < 0 || int(s.StartWindow) >= len(s.Windows) {
		return fmt.Errorf("startWindow must identify a window in the immutable schedule")
	}
	if s.MaxConcurrentLoads < 0 || s.MaxConcurrentLoads > s.Slots {
		return fmt.Errorf("maxConcurrentLoads must be between 1 and slots")
	}
	seen := map[string]bool{}
	for _, reader := range s.Readers {
		if !identifierPattern.MatchString(reader) || seen[reader] {
			return fmt.Errorf("reader IDs must be unique safe identifiers of at most 128 bytes")
		}
		seen[reader] = true
	}
	seen = map[string]bool{}
	for _, w := range s.Windows {
		if !identifierPattern.MatchString(w.ID) || seen[w.ID] || w.ExpectedBytes < 0 || w.ExpectedBytes > s.CapacityBytes {
			return fmt.Errorf("windows require unique safe IDs and expectedBytes within capacityBytes")
		}
		if !safeSource(w.Source) {
			return fmt.Errorf("window %s source must be a clean relative directory", w.ID)
		}
		seen[w.ID] = true
	}
	switch s.Backend {
	case "local":
		if len(validation.IsDNS1123Subdomain(s.SourcePVC)) != 0 || s.SourcePVC == s.PVCName {
			return fmt.Errorf("local backend requires a distinct valid sourcePVC")
		}
	case "cpfs-dataflow":
		if s.DataFlow == nil || s.DataFlow.Region == "" || s.DataFlow.FileSystemID == "" || s.DataFlow.DataFlowID == "" || s.DataFlow.FileSystemPath == "" || s.DataFlow.PVCPath == "" || len(validation.IsDNS1123Subdomain(s.DataFlow.CredentialsSecret)) != 0 {
			return fmt.Errorf("cpfs-dataflow requires complete dataFlow configuration")
		}
	default:
		return fmt.Errorf("unsupported backend %q", s.Backend)
	}
	return nil
}

var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

func safeSource(s string) bool {
	return s != "" && s != "." && !strings.HasPrefix(s, "/") && !strings.Contains(s, "\\") && !strings.ContainsRune(s, '\x00') && path.Clean(s) == s && !strings.HasPrefix(s, "../") && s != ".."
}

func freeSlot(p *api.WindowPlan) (int32, int64, int32) {
	used := map[int32]bool{}
	var reserved int64
	var loading int32
	for _, s := range p.Status.Slots {
		used[s.Index] = true
		reserved += s.ReservedBytes
		if s.Phase == "Loading" {
			loading++
		}
	}
	for i := int32(0); i < p.Spec.Slots; i++ {
		if !used[i] {
			return i, reserved, loading
		}
	}
	return -1, reserved, loading
}

func hash(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }

func LeaseName(planUID, reader string, ordinal int32, generation string) string {
	return "wl-" + hash(fmt.Sprintf("%s\n%s\n%d\n%s", planUID, reader, ordinal, generation))[:40]
}

func allReleased(p *api.WindowPlan, s *api.SlotStatus, leases []api.WindowLease) bool {
	released := map[string]bool{}
	for _, l := range leases {
		v := l.Spec
		if v.PlanName == p.Name && v.PlanUID == string(p.UID) && v.WindowIndex == s.WindowIndex && v.Generation == s.Generation && v.Released && l.Name == LeaseName(v.PlanUID, v.ReaderID, v.WindowIndex, v.Generation) {
			released[v.ReaderID] = true
		}
	}
	for _, reader := range p.Spec.Readers {
		if !released[reader] {
			return false
		}
	}
	return len(p.Spec.Readers) > 0
}

func lockName(pvc string) string { return "windowflow-pvc-" + hash(pvc)[:32] }

func (r *WindowPlanReconciler) ensurePVCLock(ctx context.Context, p *api.WindowPlan) (bool, error) {
	key := types.NamespacedName{Namespace: p.Namespace, Name: lockName(p.Spec.PVCName)}
	var lock coordv1.Lease
	if err := r.Get(ctx, key, &lock); err != nil {
		if !apierrors.IsNotFound(err) {
			return false, err
		}
		uid := string(p.UID)
		// Intentionally no ownerReference and no expiry: deleting a plan does not
		// prove its training readers or cloud transfer tasks have stopped.
		lock = coordv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace,
			Labels:      map[string]string{"app.kubernetes.io/managed-by": "windowflow"},
			Annotations: map[string]string{"data.windowflow.io/plan": p.Name, "data.windowflow.io/pvc": p.Spec.PVCName}},
			Spec: coordv1.LeaseSpec{HolderIdentity: &uid}}
		if err := r.Create(ctx, &lock); err != nil {
			if apierrors.IsAlreadyExists(err) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
	return lock.Spec.HolderIdentity != nil && *lock.Spec.HolderIdentity == string(p.UID), nil
}

func (r *WindowPlanReconciler) releasePVCLock(ctx context.Context, p *api.WindowPlan) error {
	var lock coordv1.Lease
	if err := r.Get(ctx, types.NamespacedName{Namespace: p.Namespace, Name: lockName(p.Spec.PVCName)}, &lock); err != nil {
		return client.IgnoreNotFound(err)
	}
	if lock.Spec.HolderIdentity == nil || *lock.Spec.HolderIdentity != string(p.UID) {
		return nil
	}
	return client.IgnoreNotFound(r.Delete(ctx, &lock, client.Preconditions{UID: &lock.UID, ResourceVersion: &lock.ResourceVersion}))
}

func (r *WindowPlanReconciler) ensureJob(ctx context.Context, p *api.WindowPlan, s *api.SlotStatus, action string) (*batchv1.Job, error) {
	desired, err := makeJob(p, s, action)
	if err != nil {
		return nil, err
	}
	var existing batchv1.Job
	if err := r.Get(ctx, client.ObjectKeyFromObject(desired), &existing); err == nil {
		if !metav1.IsControlledBy(&existing, p) {
			return nil, fmt.Errorf("job name collision: %s is not owned by this plan", existing.Name)
		}
		return &existing, nil
	} else if !apierrors.IsNotFound(err) {
		return nil, err
	}
	if err := r.Create(ctx, desired); err != nil {
		return nil, err
	}
	return desired, nil
}

func jobComplete(j *batchv1.Job) bool {
	for _, c := range j.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func jobFailed(j *batchv1.Job) (bool, string) {
	for _, c := range j.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return true, c.Reason + " " + c.Message
		}
	}
	return false, ""
}

func ptr[T any](v T) *T { return &v }

func makeJob(p *api.WindowPlan, s *api.SlotStatus, action string) (*batchv1.Job, error) {
	name := s.StageJob
	if action == "clean" {
		name = s.CleanJob
	}
	config := map[string]any{"action": action, "backend": p.Spec.Backend, "planUID": string(p.UID), "generation": s.Generation, "relativePath": s.RelativePath, "source": p.Spec.Windows[s.WindowIndex].Source, "expectedBytes": s.ReservedBytes}
	if d := p.Spec.DataFlow; d != nil {
		config["dataFlow"] = map[string]string{"region": d.Region, "fileSystemId": d.FileSystemID, "dataFlowId": d.DataFlowID, "fileSystemPath": d.FileSystemPath, "pvcPath": d.PVCPath}
	}
	data, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	env := []corev1.EnvVar{{Name: "WINDOWFLOW_WORKER_CONFIG", Value: string(data)}}
	if p.Spec.Backend == "cpfs-dataflow" && action == "stage" {
		for _, key := range []string{"ALIBABA_CLOUD_ACCESS_KEY_ID", "ALIBABA_CLOUD_ACCESS_KEY_SECRET", "ALIBABA_CLOUD_SECURITY_TOKEN"} {
			env = append(env, corev1.EnvVar{Name: key, ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: p.Spec.DataFlow.CredentialsSecret}, Key: key, Optional: ptr(key == "ALIBABA_CLOUD_SECURITY_TOKEN")}}})
		}
	}
	volumes := []corev1.Volume{{Name: "cache", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: p.Spec.PVCName}}}}
	mounts := []corev1.VolumeMount{{Name: "cache", MountPath: "/cache"}}
	if p.Spec.Backend == "local" && action == "stage" {
		volumes = append(volumes, corev1.Volume{Name: "source", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: p.Spec.SourcePVC, ReadOnly: true}}})
		mounts = append(mounts, corev1.VolumeMount{Name: "source", MountPath: "/source", ReadOnly: true})
	}
	deadline := p.Spec.JobTimeoutSeconds
	if deadline == 0 {
		deadline = 604800
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: p.Namespace, Labels: map[string]string{"app.kubernetes.io/name": "windowflow-worker", "data.windowflow.io/plan-uid": string(p.UID)}},
		Spec: batchv1.JobSpec{BackoffLimit: ptr(int32(0)), ActiveDeadlineSeconds: &deadline, Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app.kubernetes.io/name": "windowflow-worker"}},
			Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, AutomountServiceAccountToken: ptr(false),
				SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: ptr(true), RunAsUser: ptr(int64(1000)), RunAsGroup: ptr(int64(1000)), FSGroup: ptr(int64(1000)), FSGroupChangePolicy: ptr(corev1.FSGroupChangeOnRootMismatch)},
				Containers: []corev1.Container{{Name: action, Image: p.Spec.WorkerImage, ImagePullPolicy: corev1.PullIfNotPresent, Args: []string{action}, Env: env, VolumeMounts: mounts,
					SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: ptr(false), ReadOnlyRootFilesystem: ptr(true), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}, SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}}}}, Volumes: volumes,
			},
		}}}
	controller, block := true, true
	job.OwnerReferences = []metav1.OwnerReference{{APIVersion: api.GroupVersion.String(), Kind: "WindowPlan", Name: p.Name, UID: p.UID, Controller: &controller, BlockOwnerDeletion: &block}}
	return job, nil
}
