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
	// minimal info; controller renders full EventSource CR
	Type   string            `json:"type,omitempty"`   // e.g., "webhook"
	Params map[string]string `json:"params,omitempty"` // endpoint, method, path, etc
	// Prefer fixed NodePort to avoid read-after-write
	Port     int32 `json:"port,omitempty"`
	NodePort int32 `json:"nodePort,omitempty"`
}

type ServiceSpec struct {
	Image             string            `json:"image"`
	ConcurrencyTarget *int32            `json:"concurrencyTarget,omitempty"`
	Env               map[string]string `json:"env,omitempty"`
	// Optional: if you want to compute URL instead of reading status
	DomainSuffix string `json:"domainSuffix,omitempty"` // e.g., "example.com" or nip.io base
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

type NetworkSpec struct {
	UseGlobalService      bool   `json:"useGlobalService,omitempty"`
	GlobalAnnotationKey   string `json:"globalAnnotationKey,omitempty"`
	GlobalAnnotationValue string `json:"globalAnnotationValue,omitempty"`
}

// OrchestratorSpec defines the desired state of Orchestrator.
type OrchestratorSpec struct {
	Namespace string `json:"namespace,omitempty"`
	EventName string `json:"eventName"`
	// 기본값을 주고 싶으면 default 태그 추가
	// +kubebuilder:default=webhook
	EventType string `json:"eventType,omitempty"`

	// 옵션(컨트롤러에서 기본값 주입)
	// +kubebuilder:validation:Optional
	EventLogic string `json:"eventLogic,omitempty"`

	// 옵션(컨트롤러에서 기본값 주입)
	// 객체 필드는 pointer 로 두면 완전히 생략 가능
	// +kubebuilder:validation:Optional
	EventSource *EventSourceSpec `json:"eventSource,omitempty"`

	Service     ServiceSpec     `json:"service"` // Service.image는 필수 유지
	Placement   PlacementPolicy `json:"placementPolicy,omitempty"`
	Network     NetworkSpec     `json:"network,omitempty"`
	EventBusRef string          `json:"eventBusRef,omitempty"`
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
// +kubebuilder:validation:Enum=Pending;WaitingOnKsvc;WaitingOnNodePort;Ready;Error

// Orchestrator status는 controller가 채움
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
