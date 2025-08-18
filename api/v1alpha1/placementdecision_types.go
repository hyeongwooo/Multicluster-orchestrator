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

type ClusterScore struct {
	Cluster string `json:"cluster"`

	// 0~1000 (0.000~1.000) 범위의 밀리 단위 점수
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1000
	CPU int32 `json:"cpu,omitempty"`

	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1000
	Mem int32 `json:"mem,omitempty"`

	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1000
	GPU int32 `json:"gpu,omitempty"`

	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1000
	Lat int32 `json:"latency,omitempty"`

	// 최종 점수 (가중합 등), 동일 스케일
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1000
	Final int32 `json:"final"`
}

type SelectedCluster struct {
	Cluster  string `json:"cluster"`
	Replicas *int32 `json:"replicas,omitempty"`
}

// PlacementDecisionSpec defines the desired state of PlacementDecision.
type PlacementDecisionSpec struct {
	Preferred []string `json:"preferred,omitempty"`
}

// PlacementDecisionStatus defines the observed state of PlacementDecision.
type PlacementDecisionStatus struct {
	Scores   []ClusterScore    `json:"scores,omitempty"`
	Selected []SelectedCluster `json:"selected,omitempty"`
	Reason   string            `json:"reason,omitempty"`
	Updated  *metav1.Time      `json:"updated,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=pd

// PlacementDecision is the Schema for the placementdecisions API.
type PlacementDecision struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PlacementDecisionSpec   `json:"spec,omitempty"`
	Status PlacementDecisionStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PlacementDecisionList contains a list of PlacementDecision.
type PlacementDecisionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PlacementDecision `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PlacementDecision{}, &PlacementDecisionList{})
}
