// Copyright 2026 WindowFlow contributors. SPDX-License-Identifier: Apache-2.0
package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// DataFlowSpec identifies an existing CPFS DataFlow and the PVC's actual filesystem path.
type DataFlowSpec struct {
	Region            string `json:"region"`
	FileSystemID      string `json:"fileSystemId"`
	DataFlowID        string `json:"dataFlowId"`
	FileSystemPath    string `json:"fileSystemPath"`
	PVCPath           string `json:"pvcPath"`
	CredentialsSecret string `json:"credentialsSecret"`
}

type WindowSpec struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	ID string `json:"id"`
	// Source is a relative directory in the source PVC or DataFlow's OSS prefix.
	// +kubebuilder:validation:MinLength=1
	Source string `json:"source"`
	// ExpectedBytes is the exact sum of regular-file content bytes, not a filesystem quota.
	// +kubebuilder:validation:Minimum=0
	ExpectedBytes int64 `json:"expectedBytes"`
}

// WindowPlanSpec is immutable. A new training attempt requires a new plan.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="a WindowPlan spec is immutable; create a new plan for a new attempt"
type WindowPlanSpec struct {
	// StartWindow is the first ordinal staged by this new attempt after checkpoint recovery.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1023
	// +kubebuilder:default=0
	StartWindow int32 `json:"startWindow,omitempty"`
	// +kubebuilder:validation:MinLength=1
	PVCName string `json:"pvcName"`
	// +kubebuilder:validation:Minimum=2
	// +kubebuilder:validation:Maximum=64
	Slots int32 `json:"slots"`
	// +kubebuilder:validation:Minimum=1
	CapacityBytes int64 `json:"capacityBytes"`
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=4096
	Readers []string `json:"readers"`
	// +kubebuilder:validation:MinLength=1
	WorkerImage string `json:"workerImage"`
	// +kubebuilder:validation:Enum=local;cpfs-dataflow
	Backend string `json:"backend"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=64
	// +kubebuilder:default=1
	MaxConcurrentLoads int32         `json:"maxConcurrentLoads,omitempty"`
	SourcePVC          string        `json:"sourcePVC,omitempty"`
	DataFlow           *DataFlowSpec `json:"dataFlow,omitempty"`
	// +kubebuilder:validation:Minimum=60
	// +kubebuilder:validation:Maximum=2592000
	// +kubebuilder:default=604800
	JobTimeoutSeconds int64 `json:"jobTimeoutSeconds,omitempty"`
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=1024
	Windows []WindowSpec `json:"windows"`
}

type SlotStatus struct {
	Index       int32  `json:"index"`
	WindowIndex int32  `json:"windowIndex"`
	WindowID    string `json:"windowId"`
	Generation  string `json:"generation"`
	// +kubebuilder:validation:Enum=Loading;Ready;Reclaiming;Failed
	Phase         string `json:"phase"`
	RelativePath  string `json:"relativePath"`
	ReservedBytes int64  `json:"reservedBytes"`
	StageJob      string `json:"stageJob"`
	CleanJob      string `json:"cleanJob"`
}

type WindowPlanStatus struct {
	Phase              string       `json:"phase,omitempty"`
	Message            string       `json:"message,omitempty"`
	ObservedGeneration int64        `json:"observedGeneration,omitempty"`
	NextWindow         int32        `json:"nextWindow,omitempty"`
	CompletedWindows   int32        `json:"completedWindows,omitempty"`
	Slots              []SlotStatus `json:"slots,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Completed",type=integer,JSONPath=`.status.completedWindows`
// +kubebuilder:printcolumn:name="Slots",type=integer,JSONPath=`.spec.slots`
type WindowPlan struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              WindowPlanSpec   `json:"spec"`
	Status            WindowPlanStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type WindowPlanList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WindowPlan `json:"items"`
}

// WindowLeaseSpec records a reader's explicit release. Expiry never permits reclamation.
// +kubebuilder:validation:XValidation:rule="self.planName == oldSelf.planName && self.planUID == oldSelf.planUID && self.readerID == oldSelf.readerID && self.windowIndex == oldSelf.windowIndex && self.generation == oldSelf.generation",message="lease identity is immutable"
// +kubebuilder:validation:XValidation:rule="!oldSelf.released || self.released",message="a released lease cannot be reacquired"
type WindowLeaseSpec struct {
	PlanName string `json:"planName"`
	PlanUID  string `json:"planUID"`
	ReaderID string `json:"readerID"`
	// +kubebuilder:validation:Minimum=0
	WindowIndex int32  `json:"windowIndex"`
	Generation  string `json:"generation"`
	Released    bool   `json:"released"`
}

// +kubebuilder:object:root=true
// +kubebuilder:printcolumn:name="Reader",type=string,JSONPath=`.spec.readerID`
// +kubebuilder:printcolumn:name="Window",type=integer,JSONPath=`.spec.windowIndex`
// +kubebuilder:printcolumn:name="Released",type=boolean,JSONPath=`.spec.released`
type WindowLease struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              WindowLeaseSpec `json:"spec"`
}

// +kubebuilder:object:root=true
type WindowLeaseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WindowLease `json:"items"`
}

func init() {
	SchemeBuilder.Register(&WindowPlan{}, &WindowPlanList{}, &WindowLease{}, &WindowLeaseList{})
}
