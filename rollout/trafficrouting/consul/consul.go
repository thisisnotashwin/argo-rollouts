package consul

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/argoproj/argo-rollouts/utils/defaults"
	"github.com/argoproj/argo-rollouts/utils/record"
)

// Type holds this controller type
const Type = "Consul"

const serviceSplitters = "servicesplitters"
const serviceResolvers = "serviceresolvers"

const ServiceUpdateError = "ConsulServiceUpdateError"
const serviceMetaVersionAnnotation = "consul.hashicorp.com/service-meta-version"
const filterServiceMetaVersionTemplate = "Service.Meta.version == %s"

type ReconcilerConfig struct {
	Rollout        *v1alpha1.Rollout
	SplitterClient ClientInterface
	ResolverClient ClientInterface
	Recorder       record.EventRecorder
}

type Reconciler struct {
	Rollout        *v1alpha1.Rollout
	SplitterClient ClientInterface
	ResolverClient ClientInterface
	Recorder       record.EventRecorder
}

func (r *Reconciler) sendWarningEvent(id, msg string) {
	r.sendEvent(corev1.EventTypeWarning, id, msg)
}

func (r *Reconciler) sendEvent(eventType, id, msg string) {
	r.Recorder.Eventf(r.Rollout, record.EventOptions{EventType: eventType, EventReason: id}, msg)
}

type ClientInterface interface {
	Get(ctx context.Context, name string, options metav1.GetOptions, subresources ...string) (*unstructured.Unstructured, error)
	Update(ctx context.Context, obj *unstructured.Unstructured, options metav1.UpdateOptions, subresources ...string) (*unstructured.Unstructured, error)
}

func NewReconciler(cfg *ReconcilerConfig) *Reconciler {
	reconciler := &Reconciler{
		Rollout:        cfg.Rollout,
		SplitterClient: cfg.SplitterClient,
		ResolverClient: cfg.ResolverClient,
		Recorder:       cfg.Recorder,
	}
	return reconciler
}

func NewSplitterDynamicClient(di dynamic.Interface, namespace string) dynamic.ResourceInterface {
	resourceSchema := schema.GroupVersionResource{
		Group:    defaults.DefaultConsulAPIGroup,
		Version:  defaults.DefaultConsulVersion,
		Resource: serviceSplitters,
	}
	return di.Resource(resourceSchema).Namespace(namespace)
}

func NewResolverDynamicClient(di dynamic.Interface, namespace string) dynamic.ResourceInterface {
	resourceSchema := schema.GroupVersionResource{
		Group:    defaults.DefaultConsulAPIGroup,
		Version:  defaults.DefaultConsulVersion,
		Resource: serviceResolvers,
	}
	return di.Resource(resourceSchema).Namespace(namespace)
}

func (r *Reconciler) UpdateHash(_, _ string, _ ...v1alpha1.WeightDestination) error {
	return nil
}

func (r *Reconciler) SetWeight(desiredWeight int32, _ ...v1alpha1.WeightDestination) error {
	ctx := context.TODO()
	rollout := r.Rollout

	serviceName := rollout.Spec.Strategy.Canary.TrafficRouting.Consul.ServiceName
	canarySubsetName := rollout.Spec.Strategy.Canary.TrafficRouting.Consul.CanarySubsetName
	stableSubsetName := rollout.Spec.Strategy.Canary.TrafficRouting.Consul.StableSubsetName
	serviceMetaVersion := rollout.Spec.Template.GetObjectMeta().GetAnnotations()[serviceMetaVersionAnnotation]

	// If the rollout is successful (not aborted) then modify the resolver
	if rollout.Status.Canary == (v1alpha1.CanaryStatus{}) {
		return nil
	}

	// Check if the pods have completely rolled over, and we are finished, now set the resolver to the stable version
	if r.rolloutAborted() {
		err := r.updateResolverForAbortedRollout(ctx, serviceName, canarySubsetName)
		if err != nil {
			return err
		}
	} else {
		if r.rolloutComplete() {
			err := r.updateResolverAfterCompletion(ctx, serviceName, stableSubsetName, canarySubsetName, serviceMetaVersion)
			if err != nil {
				return err
			}
		} else {
			err := r.updateResolverForRollouts(ctx, serviceName, canarySubsetName, serviceMetaVersion)
			if err != nil {
				return err
			}
		}
	}

	serviceSplitter, err := r.SplitterClient.Get(ctx, serviceName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	splits, isFound, err := unstructured.NestedSlice(serviceSplitter.Object, "spec", "splits")

	// Set Weight for Canary Services
	if err != nil {
		return err
	}
	if !isFound {
		return errors.New("spec.splits was not found in consul service splitter")
	}
	canaryService, err := getService(canarySubsetName, splits)
	if err != nil {
		return err
	}
	if canaryService == nil {
		return errors.New("consul canary service was not found")
	}
	err = unstructured.SetNestedField(canaryService, int64(desiredWeight), "weight")
	if err != nil {
		return err
	}

	// Set Weight for Stable Services
	stableService, err := getService(stableSubsetName, splits)
	if err != nil {
		return err
	}
	if stableService == nil {
		return errors.New("consul stable service was not found")
	}
	err = unstructured.SetNestedField(stableService, int64(100-desiredWeight), "weight")
	if err != nil {
		return err
	}

	// Persist changes to the ServiceSplitter
	err = unstructured.SetNestedSlice(serviceSplitter.Object, splits, "spec", "splits")
	if err != nil {
		return err
	}
	_, err = r.SplitterClient.Update(ctx, serviceSplitter, metav1.UpdateOptions{})
	if err != nil {
		msg := fmt.Sprintf("Error updating consul service splitter %q: %s", serviceSplitter.GetName(), err)
		r.sendWarningEvent(ServiceUpdateError, msg)
	}
	return err
}

func (r *Reconciler) rolloutComplete() bool {
	rollout := r.Rollout
	rolloutCondition, err := r.CompleteCondition(rollout)
	if err != nil {
		return false
	}
	return strconv.FormatInt(rollout.GetObjectMeta().GetGeneration(), 10) == rollout.Status.ObservedGeneration &&
		rolloutCondition.Status == corev1.ConditionTrue
}

func (r *Reconciler) CompleteCondition(rollout *v1alpha1.Rollout) (v1alpha1.RolloutCondition, error) {
	for i, condition := range rollout.Status.Conditions {
		if condition.Type == v1alpha1.RolloutCompleted {
			return rollout.Status.Conditions[i], nil
		}
	}
	return v1alpha1.RolloutCondition{}, errors.New("condition RolloutCompleted not found")
}

func (r *Reconciler) rolloutAborted() bool {
	rollout := r.Rollout
	return rollout.Status.Abort
}

func (r *Reconciler) updateResolverAfterCompletion(ctx context.Context, serviceName, stableSubsetName, canarySubsetName, serviceMetaVersion string) error {
	// Read service resolver and set the filter for the canary subset
	serviceResolver, err := r.ResolverClient.Get(ctx, serviceName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	// Check that the filter field exists
	_, isFound, err := unstructured.NestedString(serviceResolver.Object, "spec", "subsets", stableSubsetName, "filter")
	if err != nil {
		return err
	}
	if !isFound {
		return errors.New(fmt.Sprintf("spec.subsets.%s.filter was not found in consul service resolver: %v", stableSubsetName, serviceResolver))
	}

	// Persist changes to the resolver
	err = unstructured.SetNestedField(serviceResolver.Object, fmt.Sprintf(filterServiceMetaVersionTemplate, serviceMetaVersion), "spec", "subsets", stableSubsetName, "filter")
	if err != nil {
		return err
	}

	// Set the filter for the canary subset name to empty
	_, isFound, err = unstructured.NestedMap(serviceResolver.Object, "spec", "subsets", canarySubsetName)
	if err != nil {
		return err
	}
	if !isFound {
		return errors.New(fmt.Sprintf("spec.subsets.%s.filter was not found in consul service resolver: %v", canarySubsetName, serviceResolver))
	}
	err = unstructured.SetNestedField(serviceResolver.Object, "", "spec", "subsets", canarySubsetName, "filter")
	if err != nil {
		return err
	}

	_, err = r.ResolverClient.Update(ctx, serviceResolver, metav1.UpdateOptions{})
	if err != nil {
		msg := fmt.Sprintf("Error updating consul service resolver %q: %s", serviceResolver.GetName(), err)
		r.sendWarningEvent(ServiceUpdateError, msg)
	}
	return nil
}

func (r *Reconciler) updateResolverForRollouts(ctx context.Context, serviceName, canarySubsetName, serviceMetaVersion string) error {
	// Read service resolver and set the filter for the canary subset
	serviceResolver, err := r.ResolverClient.Get(ctx, serviceName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	// Set the filter for the canary subset name to empty
	val, isFound, err := unstructured.NestedString(serviceResolver.Object, "spec", "subsets", canarySubsetName, "filter")
	if err != nil {
		return err
	}
	if !isFound {
		return errors.New(fmt.Sprintf("spec.subsets.%s.filter was not found in consul service resolver: %v", canarySubsetName, serviceResolver))
	}

	if val == fmt.Sprintf(filterServiceMetaVersionTemplate, serviceMetaVersion) {
		return nil
	}

	// Persist changes to the resolver
	err = unstructured.SetNestedField(serviceResolver.Object, fmt.Sprintf(filterServiceMetaVersionTemplate, serviceMetaVersion), "spec", "subsets", canarySubsetName, "filter")
	if err != nil {
		return err
	}

	_, err = r.ResolverClient.Update(ctx, serviceResolver, metav1.UpdateOptions{})
	if err != nil {
		msg := fmt.Sprintf("Error updating consul service resolver %q: %s", serviceResolver.GetName(), err)
		r.sendWarningEvent(ServiceUpdateError, msg)
	}
	return nil
}

func (r *Reconciler) updateResolverForAbortedRollout(ctx context.Context, serviceName, canarySubsetName string) error {
	// Read service resolver and set the filter for the canary subset
	serviceResolver, err := r.ResolverClient.Get(ctx, serviceName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	// Set the filter for the canary subset name to empty
	_, isFound, err := unstructured.NestedString(serviceResolver.Object, "spec", "subsets", canarySubsetName, "filter")
	if err != nil {
		return err
	}
	if !isFound {
		return errors.New(fmt.Sprintf("spec.subsets.%s.filter was not found in consul service resolver: %v", canarySubsetName, serviceResolver))
	}

	// Persist changes to the resolver
	err = unstructured.SetNestedField(serviceResolver.Object, "", "spec", "subsets", canarySubsetName, "filter")
	if err != nil {
		return err
	}

	_, err = r.ResolverClient.Update(ctx, serviceResolver, metav1.UpdateOptions{})
	if err != nil {
		msg := fmt.Sprintf("Error updating consul service resolver %q: %s", serviceResolver.GetName(), err)
		r.sendWarningEvent(ServiceUpdateError, msg)
	}
	return nil
}

func (r *Reconciler) SetHeaderRoute(headerRouting *v1alpha1.SetHeaderRoute) error {
	return nil
}

func (r *Reconciler) VerifyWeight(desiredWeight int32, additionalDestinations ...v1alpha1.WeightDestination) (*bool, error) {
	return nil, nil
}

func (r *Reconciler) Type() string {
	return Type
}

func (r *Reconciler) SetMirrorRoute(setMirrorRoute *v1alpha1.SetMirrorRoute) error {
	return nil
}

func (r *Reconciler) RemoveManagedRoutes() error {
	return nil
}

func getService(desiredServiceName string, services []any) (map[string]any, error) {
	var selectedService map[string]any
	for _, service := range services {
		typedService, ok := service.(map[string]any)
		if !ok {
			return nil, errors.New("failed type assertion setting weight for consul service splitter")
		}
		serviceSubsetName, isFound, err := unstructured.NestedString(typedService, "serviceSubset")
		if err != nil {
			return nil, err
		}
		if !isFound {
			return nil, errors.New("name field was not found in service")
		}
		if serviceSubsetName == desiredServiceName {
			selectedService = typedService
			break
		}
	}
	return selectedService, nil
}
