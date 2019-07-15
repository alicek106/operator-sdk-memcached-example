package memcached

import (
	"context"
	"reflect"

	// cachev1alpha1 "github.com/example-inc/memcached-operator/pkg/apis/cache/v1alpha1"
	cachev1alpha1 "memcached-operator/pkg/apis/cache/v1alpha1"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	logf "sigs.k8s.io/controller-runtime/pkg/runtime/log"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logf.Log.WithName("controller_memcached")

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// Memcached 컨트롤러를 매니저에 추가하는 부분입니다. 중요하지 않음
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// new reconcile.Reconciler 생성자입니다. 중요하지 않음
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcileMemcached{client: mgr.GetClient(), scheme: mgr.GetScheme()}
}

// 매니저에 새로운 컨트롤러를 추가합니다. 볼 필요는 있지만 딱히 중요하진 않음
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// 새 컨트롤러 생성
	c, err := controller.New("memcached-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Memcached 커스텀 리소스에 대해 Watch를 겁니다.
	err = c.Watch(&source.Kind{Type: &cachev1alpha1.Memcached{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// TODO(user): Modify this to be the types you create that are owned by the primary resource
	// Watch for changes to secondary resource Pods and requeue the owner Memcached
	err = c.Watch(&source.Kind{Type: &appsv1.Deployment{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &cachev1alpha1.Memcached{},
	})
	if err != nil {
		return err
	}

	return nil
}

// blank assignment to verify that ReconcileMemcached implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileMemcached{}

// ReconcileMemcached reconciles a Memcached object
type ReconcileMemcached struct {
	// TODO: Clarify the split client
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client client.Client
	scheme *runtime.Scheme
}

// 매우 중요함
// Reconcile 함수는 현재 클러스터의 상태를 읽어들인 뒤, 커스텀 리소스가 바람직한 상태가 될 수 있도록 변경하는 작업을 수행합니다.
// 예를 들어, A라는 포드가 생성되어 있는 것이 바람직하다면, A 포드가 존재하는지 확인하고, 없다면 새로 생성합니다.
// 따라서 이 부분에 여러분의 비즈니스 로직을 직접 구현하면 됩니다.
func (r *ReconcileMemcached) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("Reconciling Memcached")

	// 가장 먼저, 큐에서 전달된, 변경 사항이 발생한 Memcached를 쿠버네티스로부터 가져옵니다.
	// request.NamespacedName이기 때문에, Get 인자로서 default/myname 과 같은 식으로 들어갑니다.
	err := r.client.Get(context.TODO(), request.NamespacedName, memcached) 
	if err != nil { // 변경 사항이 발생한 Memcached가 발견되지 않았습니다.
		if errors.IsNotFound(err) {
			// 변경 사항이 발생했는데 쿠버네티스에 존재하지 않는다는 것은 삭제되었다는 것을 뜻합니다.
			// 따라서 (삭제) 변경사항이 발생했고, 그 Memcached는 이미 클러스터에 존재하지 않습니다.
			// 필요하다면 finalizer를 사용해 별도의 삭제 로직을 구현할 수도 있다고 합니다.
			reqLogger.Info("Memcached resource not found. Ignoring since object must be deleted")

			// 리턴 값은 두개로 구성되어 있습니다. 이 리턴 값은 이 이벤트를 다시 큐에 enqueue할지를 결정합니다.
			// 우선 2번째 반환값인 error가 nil이 아니면 이 이벤트는 다시 큐에 enqueue 됩니다.
			// 또는 reconcile.Result{} 가 true로 설정되었을 때에도 다시 enqueue 됩니다.
			// 왜 이벤트를 굳이 다시 enqueue하는지에 대해서는 아래에서 다시 설명합니다.
			return reconcile.Result{}, nil 
		}
		// Get 함수 에러 처리입니다.
		reqLogger.Error(err, "Failed to get Memcached")
		return reconcile.Result{}, err
	}

	// Memcached를 위한 디플로이먼트가 생성되어 있는지 확인합니다. 
	// 즉, Memcached의 바람직한 상태는 Memecached를 위한 Deployment가 생성되어 있는 상태입니다.
	found := &appsv1.Deployment{}
	err = r.client.Get(context.TODO(), types.NamespacedName{Name: memcached.Name, Namespace: memcached.Namespace}, found)
	if err != nil && errors.IsNotFound(err) { // 만약에 Memcached를 위한 Deployment가 없다면
		// 새로운 Deployment를 생성합니다. deploymentForMemcached 함수는 Deployment를 위한 spec을 반환합니다.
		dep := r.deploymentForMemcached(memcached)
		reqLogger.Info("Creating a new Deployment", "Deployment.Namespace", dep.Namespace, "Deployment.Name", dep.Name)
		err = r.client.Create(context.TODO(), dep)
		if err != nil {
			reqLogger.Error(err, "Failed to create new Deployment", "Deployment.Namespace", dep.Namespace, "Deployment.Name", dep.Name)
			return reconcile.Result{}, err
		}
		// Deployment가 성공적으로 생성되었다면, 이 이벤트를 다시 Requeue 합니다.
		return reconcile.Result{Requeue: true}, nil
	} else if err != nil {
		reqLogger.Error(err, "Failed to get Deployment")
		return reconcile.Result{}, err
	}

	// Requeue된 이벤트는  (Deployment가 이미 생성되어 있기 때문에 위의 if문을 그냥 지나칠테고, 아래의 소스코드를 실행합니다.
	// Deployment의 size를 Memcached의 size에 맞도록 scaling 합니다. 
	// 즉, memcached를 생성할 때 사용한 YAML 파일의 size 값을 사용하는 것입니다.
	size := memcached.Spec.Size
	if *found.Spec.Replicas != size {
		found.Spec.Replicas = &size
		err = r.client.Update(context.TODO(), found)
		if err != nil {
			reqLogger.Error(err, "Failed to update Deployment", "Deployment.Namespace", found.Namespace, "Deployment.Name", found.Name)
			return reconcile.Result{}, err
		}
		// 레플리카 갯수가 업데이트 되었다면, 다시 requeue 합니다.
		return reconcile.Result{Requeue: true}, nil
	}

	// Memcached의 Status를 각 파드의 이름으로 업데이트합니다.
	// kubectl describe memcached를 해보면 Status 항목이 정의되어 있을 것입니다.
	podList := &corev1.PodList{}
	labelSelector := labels.SelectorFromSet(labelsForMemcached(memcached.Name))
	listOps := &client.ListOptions{Namespace: memcached.Namespace, LabelSelector: labelSelector}
	err = r.client.List(context.TODO(), listOps, podList)
	if err != nil {
		reqLogger.Error(err, "Failed to list pods", "Memcached.Namespace", memcached.Namespace, "Memcached.Name", memcached.Name)
		return reconcile.Result{}, err
	}
	podNames := getPodNames(podList.Items)

	// Update status.Nodes if needed
	if !reflect.DeepEqual(podNames, memcached.Status.Nodes) {
		memcached.Status.Nodes = podNames
		err := r.client.Status().Update(context.TODO(), memcached)
		if err != nil {
			reqLogger.Error(err, "Failed to update Memcached status")
			return reconcile.Result{}, err
		}
	}

	return reconcile.Result{}, nil
}

// deploymentForMemcached returns a memcached Deployment object
func (r *ReconcileMemcached) deploymentForMemcached(m *cachev1alpha1.Memcached) *appsv1.Deployment {
	ls := labelsForMemcached(m.Name)
	replicas := m.Spec.Size

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      m.Name,
			Namespace: m.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: ls,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: ls,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Image:   "memcached:1.4.36-alpine",
						Name:    "memcached",
						Command: []string{"memcached", "-m=64", "-o", "modern", "-v"},
						Ports: []corev1.ContainerPort{{
							ContainerPort: 11211,
							Name:          "memcached",
						}},
					}},
				},
			},
		},
	}
	// Set Memcached instance as the owner and controller
	controllerutil.SetControllerReference(m, dep, r.scheme)
	return dep
}

// labelsForMemcached returns the labels for selecting the resources
// belonging to the given memcached CR name.
func labelsForMemcached(name string) map[string]string {
	return map[string]string{"app": "memcached", "memcached_cr": name}
}

// getPodNames returns the pod names of the array of pods passed in
func getPodNames(pods []corev1.Pod) []string {
	var podNames []string
	for _, pod := range pods {
		podNames = append(podNames, pod.Name)
	}
	return podNames
}
