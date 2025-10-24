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

package controller

import (
	"context"
	"fmt"
	"time"

	autoscalingv1 "github.com/boqier/cronhpa/api/v1"
	"github.com/robfig/cron/v3"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// CronHPAReconciler reconciles a CronHPA object
type CronHPAReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=autoscaling.aiops.com,resources=cronhpas,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=autoscaling.aiops.com,resources=cronhpas/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=autoscaling.aiops.com,resources=cronhpas/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the CronHPA object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.19.1/pkg/reconcile
func (r *CronHPAReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("reconciling cronhpa")
	var cronhpa autoscalingv1.CronHPA
	if err := r.Get(ctx, req.NamespacedName, &cronhpa); err != nil {
		if apierrors.IsNotFound(err) {
			// resource not found — nothing to do
			return ctrl.Result{}, nil
		}
		// requeue on other errors
		return ctrl.Result{}, err
	}
	now := time.Now()
	var earliestNextRuntime *time.Time
	for _, job := range cronhpa.Spec.Jobs {

		lastRunnertime := cronhpa.Status.LastRunTimes[job.Name]
		nextSchedulerTime, err := r.getNextSchedulerdTime(job.Schedule, lastRunnertime.Time)
		if err != nil {
			log.Error(err, "file to calculate next schedule time")
			return reconcile.Result{}, err
		}
		log.Info("job info", "name:", job.Name, "lastRuntime:", lastRunnertime, "nextscheduleTime:", nextSchedulerTime, "now", now)
		//检测现在是否已经到了运行时间
		if now.After(nextSchedulerTime) || now.Equal(nextSchedulerTime) {
			log.Info("updating deployment replicas", "name", cronhpa.Spec.ScaleTargetRef.Name, "targetSize:", job.TargetSize)
			if err := r.updateDeploymentReplicas(ctx, &cronhpa, cronhpa.Spec.ScaleTargetRef, job); err != nil {
				return reconcile.Result{}, err
			}
			//更新作业的最后一次运行时间
			cronhpa.Status.CurrentReplicas = job.TargetSize
			cronhpa.Status.LastScaleTime = &metav1.Time{Time: now}
			//计算下一次运行时间
			nextRuntime, _ := r.getNextSchedulerdTime(job.Schedule, now)
			if earliestNextRuntime == nil || nextRuntime.Before(*earliestNextRuntime) {
				earliestNextRuntime = &nextRuntime
			}
		}
	}
	if earliestNextRuntime != nil {
		requeueAfter := earliestNextRuntime.Sub(now)
		if requeueAfter < 0 {
			requeueAfter = time.Second
		}
		log.Info("requeue after", "time", requeueAfter)
		return reconcile.Result{RequeueAfter: requeueAfter}, nil
	}

	return ctrl.Result{}, nil
}

func (r *CronHPAReconciler) updateDeploymentReplicas(ctx context.Context, cronhpa *autoscalingv1.CronHPA, scaleTargetRef autoscalingv1.ScaleTargetReference, job autoscalingv1.JobSpec) error {
	log := log.FromContext(ctx)
	deployment := &appsv1.Deployment{}
	deploymentkey := types.NamespacedName{
		Name:      scaleTargetRef.Name,
		Namespace: cronhpa.Namespace,
	}
	if err := r.Get(ctx, deploymentkey, deployment); err != nil {
		if apierrors.IsNotFound(err) {
			log.Error(err, "deployment not found", "deployment", deploymentkey)
		}
		return fmt.Errorf("failed to get deployment %w", err)
	}
	if deployment.Spec.Replicas != nil && *&deployment.Spec.Replicas == &job.TargetSize {
		log.Info("reployment has at describe count", "deployment", deployment, "replicas", job.TargetSize)
		return nil
	}
	deployment.Spec.Replicas = &job.TargetSize
	if err := r.Update(ctx, deployment); err != nil {
		return fmt.Errorf("failed to update deplotment %w", err)

	}
	log.Info("succed update deployment replicas:", "deployment:", deployment, "replicas:", job.TargetSize)
	return nil
}

func (r *CronHPAReconciler) getNextSchedulerdTime(scheduler string, after time.Time) (time.Time, error) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(scheduler)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse schedule %s : %v", scheduler, err)
	}
	nextTime := schedule.Next(after)
	return nextTime, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *CronHPAReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&autoscalingv1.CronHPA{}).
		Named("cronhpa").
		Complete(r)
}
