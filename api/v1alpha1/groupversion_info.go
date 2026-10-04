// Copyright 2026 WindowFlow contributors. SPDX-License-Identifier: Apache-2.0
// Package v1alpha1 contains the WindowFlow API.
// +kubebuilder:object:generate=true
// +groupName=data.windowflow.io
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	GroupVersion  = schema.GroupVersion{Group: "data.windowflow.io", Version: "v1alpha1"}
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}
	AddToScheme   = SchemeBuilder.AddToScheme
)
