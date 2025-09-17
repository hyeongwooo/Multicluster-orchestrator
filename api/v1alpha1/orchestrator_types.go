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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.
type EventSourceSpec struct {
	// +kubebuilder:validation:Required
	// minimal info; controller renders full EventSource CR
	Name string `json:"name"`
	// +kubebuilder:default=webhook
	Type   string            `json:"type,omitempty"`   // e.g., "webhook"
	Params map[string]string `json:"params,omitempty"` // endpoint, method, path, etc
	// Prefer fixed NodePort to avoid read-after-write
	Port     int32 `json:"port,omitempty"`
	NodePort int32 `json:"nodePort,omitempty"`
}

// ServiceSpec describes a single Knative Service to be created.
type ServiceSpec struct {
	// ServiceSpec can be used in a list of services.
	Name              string            `json:"name,omitempty"` // optional logical name (needed when using multiple services)
	Image             string            `json:"image"`
	ConcurrencyTarget *int32            `json:"concurrencyTarget,omitempty"`
	Env               map[string]string `json:"env,omitempty"`
	// Optional: if you want to compute URL instead of reading status
	DomainSuffix string `json:"domainSuffix,omitempty"` // e.g., "example.com" or nip.io base
}

// SensorSpec defines a simplified DSL for sensors (optional; controller can derive defaults).
type SensorSpec struct {
	Name      string   `json:"name"`                 // sensor 이름
	DependsOn []string `json:"dependsOn,omitempty"`  // 의존하는 이벤트소스들
	Logic     string   `json:"logic,omitempty"`      // 조건식 (ex: "A || B")
	Workflow  string   `json:"workflowTemplateName"` // 트리거할 WorkflowTemplate 이름
}

type PlacementPolicy struct {
	AllowedClusters []string `json:"allowedClusters,omitempty"`
	DeniedClusters  []string `json:"deniedClusters,omitempty"`
	MinGPU          *int32   `json:"minGPU,omitempty"`
	// Weights for scoring
	CPUWeight     *int32 `json:"cpuWeight,omitempty"`
	MemWeight     *int32 `json:"memWeight,omitempty"`
	GPUWeight     *int32 `json:"gpuWeight,omitempty"`
	LatencyWeight *int32 `json:"latencyWeight,omitempty"`
}

// OrchestratorSpec defines the desired state of Orchestrator.
type OrchestratorSpec struct {
	Namespace string `json:"namespace,omitempty"`
	EventName string `json:"eventName"`
	// +kubebuilder:default=webhook
	EventType string `json:"eventType,omitempty"`

	// Optional high-level condition expression for sensor (e.g., "A || B").
	// +kubebuilder:validation:Optional
	EventLogic string `json:"eventLogic,omitempty"`

	// 객체 필드는 pointer 로 두면 완전히 생략 가능
	// +kubebuilder:validation:Optional
	EventSource *EventSourceSpec `json:"eventSource,omitempty"`
	// +kubebuilder:validation:Optional
	EventSources []EventSourceSpec `json:"eventSources,omitempty"`

	// One or more Knative Services. When set, the controller should prefer this field.
	// +kubebuilder:validation:Optional
	// If empty, fallback to single Service for backward-compat.
	// +kubebuilder:validation:Optional
	Services []ServiceSpec `json:"services,omitempty"`

	// +kubebuilder:validation:Optional
	// Deprecated: prefer `services` when you need multiple kservices.
	Service ServiceSpec `json:"service,omitempty"`

	Placement PlacementPolicy `json:"placementPolicy,omitempty"`
	// +kubebuilder:validation:Optional
	Sensor *SensorSpec `json:"sensor,omitempty"`
}

// OrchestratorStatus defines the observed state of Orchestrator.

// OrchestratorStatus defines the observed state of Orchestrator.
type OrchestratorPhase string

const (
	PhasePending           OrchestratorPhase = "Pending"
	PhaseWaitingOnKsvc     OrchestratorPhase = "WaitingOnKsvc"
	PhaseWaitingOnNodePort OrchestratorPhase = "WaitingOnNodePort"
	PhaseReady             OrchestratorPhase = "Ready"
	PhaseError             OrchestratorPhase = "Error"
)

// OrchestratorStatus holds runtime status fields.
type OrchestratorStatus struct {
	// +kubebuilder:validation:Enum=Pending;WaitingOnKsvc;WaitingOnNodePort;Ready;Error
	Phase OrchestratorPhase `json:"phase,omitempty"`

	Conditions []metav1.Condition `json:"conditions,omitempty"`

	KsvcURL    string `json:"ksvcURL,omitempty"`
	ESNodePort int32  `json:"eventSourceNodePort,omitempty"`

	SelectedClusters  []string     `json:"selectedClusters,omitempty"`
	LastPlacementTime *metav1.Time `json:"lastPlacementTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=orch
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="KsvcURL",type=string,JSONPath=`.status.ksvcURL`
// +kubebuilder:printcolumn:name="NodePort",type=integer,JSONPath=`.status.eventSourceNodePort`

// Orchestrator is the Schema for the orchestrators API.
type Orchestrator struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   OrchestratorSpec   `json:"spec,omitempty"`
	Status OrchestratorStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// OrchestratorList contains a list of Orchestrator.
type OrchestratorList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Orchestrator `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Orchestrator{}, &OrchestratorList{})
}
