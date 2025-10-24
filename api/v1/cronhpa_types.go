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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// CronHPASpec defines the desired state of CronHPA.
type CronHPASpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Foo is an example field of CronHPA. Edit cronhpa_types.go to remove/update
	ScaleTargetRef ScaleTargetReference `json:"scaleTargetRef"`
	Jobs           []JobSpec            `json:"jobs"`
}
type ScaleTargetReference struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
}
type JobSpec struct {
	Name       string `json:"name"`
	Schedule   string `json:"schedule"`
	TargetSize int32  `json:"targetSize"`
}

// CronHPAStatus defines the observed state of CronHPA.
type CronHPAStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	CurrentReplicas int32                  `json:"currentReplicas,omitempty"`
	LastScaleTime   *metav1.Time           `json:"lastScaleTime,omitempty"`
	LastRunTimes    map[string]metav1.Time `json:"lastRunTimes,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// 这是用来控制 additionalPrinterColumns 的注释，用来控制 kubectl get cronhpa 的输出
// +kubebuilder:printcolumn:name="Target",type="string",JSONPath=".spec.scaleTargetRef.name",description="目标工作负载"
// +kubebuilder:printcolumn:name="Schedule",type="string",JSONPath=".spec.jobs[*].schedule",description="Cron 表达式"
// +kubebuilder:printcolumn:name="Target Size",type="integer",JSONPath=".spec.jobs[*].targetSize",description="目标副本数"
// CronHPA is the Schema for the cronhpas API.
type CronHPA struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CronHPASpec   `json:"spec,omitempty"`
	Status CronHPAStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// CronHPAList contains a list of CronHPA.
type CronHPAList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CronHPA `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CronHPA{}, &CronHPAList{})
}
