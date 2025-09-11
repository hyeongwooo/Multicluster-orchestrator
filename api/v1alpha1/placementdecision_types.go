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

// ---- 점수/선택 결과 ----

type ClusterScore struct {
	Cluster string `json:"cluster"`
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
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1000
	Final int32 `json:"final"`
}

type SelectedCluster struct {
	Cluster  string `json:"cluster"`
	Replicas *int32 `json:"replicas,omitempty"`
}

// 가중치 설정 (모두 0~1000 범위의 밀리 스케일; 합이 1000일 필요는 없음)
type PlacementWeight struct {
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
	Latency int32 `json:"latency,omitempty"`

	// 선택 유지 히스테리시스 임계값 (점수 차이 이 값 미만이면 기존 선택 유지)
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1000
	Hysteresis int32 `json:"hysteresis,omitempty"`
}

// ---- Spec/Status ----

type PlacementDecisionSpec struct {
	Preferred       []string        `json:"preferred,omitempty"`
	AllowedClusters []string        `json:"allowedClusters,omitempty"`
	Weights         PlacementWeight `json:"weights,omitempty"`
}

type PlacementDebug struct {
	PromURL string            `json:"promURL,omitempty"`
	Queries map[string]string `json:"queries,omitempty"`
}

// ★ EventInfraStatus는 중첩 타입(리소스 아님). 절대 object:root 마커 넣지 않기!
type EventInfraStatus struct {
	EventBusHome          string            `json:"eventBusHome,omitempty"`
	GlobalNATSServiceName string            `json:"globalNatsServiceName,omitempty"`
	NatsBackendSelector   map[string]string `json:"natsBackendSelector,omitempty"`
	BusURL                string            `json:"busURL,omitempty"`
}

type PlacementDecisionStatus struct {
	Selected   []SelectedCluster `json:"selected,omitempty"`
	Scores     []ClusterScore    `json:"scores,omitempty"`
	Reason     string            `json:"reason,omitempty"`
	Updated    *metav1.Time      `json:"updated,omitempty"`
	EventInfra *EventInfraStatus `json:"eventInfra,omitempty"`
	Debug      *PlacementDebug   `json:"debug,omitempty"`
}

// ---- CRD 루트 객체들만 object:root 마커 ----

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=pd
type PlacementDecision struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PlacementDecisionSpec   `json:"spec,omitempty"`
	Status PlacementDecisionStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type PlacementDecisionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PlacementDecision `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PlacementDecision{}, &PlacementDecisionList{})
}
