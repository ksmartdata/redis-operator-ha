package service

import (
	"fmt"
	"reflect"
	"strings"

	redisfailoverv1 "github.com/spotahome/redis-operator/api/redisfailover/v1"
	"github.com/spotahome/redis-operator/log"
	"github.com/spotahome/redis-operator/metrics"
	"github.com/spotahome/redis-operator/operator/redisfailover/util"
	"github.com/spotahome/redis-operator/service/k8s"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	advertisedPortsConfigMapNameSuffix = "-advertised-ports"
	nodePortSuffix                     = "-nodeport"
)

// RedisFailoverKubeClientCustom is our implementation of RedisFailoverClient interface with custom functionality
type RedisFailoverKubeClientCustom struct {
	RedisFailoverKubeClient
}

// NewRedisFailoverKubeClientCustom creates a new RedisFailoverKubeClientCustom
func NewRedisFailoverKubeClientCustom(k8sService k8s.Services, logger log.Logger, metricsClient metrics.Recorder) *RedisFailoverKubeClientCustom {
	logger = logger.With("service", "redis.clientcustom")
	return &RedisFailoverKubeClientCustom{
		RedisFailoverKubeClient: RedisFailoverKubeClient{
			K8SService:    k8sService,
			logger:        logger,
			metricsClient: metricsClient,
		},
	}
}

// EnsureRedisStatefulset makes sure the redis statefulset exists in the desired state
// with custom modifications for advertised ports
func (r *RedisFailoverKubeClientCustom) EnsureRedisStatefulset(rf *redisfailoverv1.RedisFailover, labels map[string]string, ownerRefs []metav1.OwnerReference) error {
	if !rf.Spec.Redis.DisablePodDisruptionBudget {
		if err := r.ensurePodDisruptionBudget(rf, redisName, redisRoleName, labels, ownerRefs); err != nil {
			return err
		}
	}
	ss := generateRedisStatefulSet(rf, labels, ownerRefs)
	if _, ok := rf.Annotations[AnnotationKeyAdvertisedPort]; ok {
		ss = decorateRedisStatefulSet(rf, ss)
	}

	// Create or update the statefulset
	err := r.K8SService.CreateOrUpdateStatefulSet(rf.Namespace, ss)
	r.setEnsureOperationMetrics(ss.Namespace, ss.Name, "StatefulSet", rf.Name, err)
	return err
}

// EnsureRedisConfigMap makes sure the Redis ConfigMap exists
// and also creates advertised ports ConfigMap if annotation exists
func (r *RedisFailoverKubeClientCustom) EnsureRedisConfigMap(rf *redisfailoverv1.RedisFailover, labels map[string]string, ownerRefs []metav1.OwnerReference) error {
	if err := r.RedisFailoverKubeClient.EnsureRedisConfigMap(rf, labels, ownerRefs); err != nil {
		return err
	}

	// If advertised port annotation exists, create the advertised ports ConfigMap
	if _, ok := rf.Annotations[AnnotationKeyAdvertisedPort]; ok {
		apcm := generateRedisAdvertisedPortsConfigMap(rf, labels, ownerRefs)
		if err := r.K8SService.CreateOrUpdateConfigMap(rf.Namespace, apcm); err != nil {
			r.setEnsureOperationMetrics(apcm.Namespace, apcm.Name, "ConfigMap", rf.Name, err)
			return err
		}
		r.setEnsureOperationMetrics(apcm.Namespace, apcm.Name, "ConfigMap", rf.Name, nil)
	}
	return nil
}

func generateRedisAdvertisedPortsConfigMap(rf *redisfailoverv1.RedisFailover, labels map[string]string, ownerRefs []metav1.OwnerReference) *corev1.ConfigMap {
	// Parse annotation value to get pod name to port mappings
	annoVal, ok := rf.Annotations[AnnotationKeyAdvertisedPort]
	if !ok {
		return nil
	}

	// Parse the annotation value
	// Format: "rfr-redis-0:22220,rfr-redis-1:22221,rfr-redis-2:22222"
	pairs := strings.Split(annoVal, ",")
	if len(pairs) == 0 {
		return nil
	}

	// Create ConfigMap data
	data := make(map[string]string)
	for _, pair := range pairs {
		parts := strings.Split(pair, ":")
		if len(parts) != 2 {
			continue
		}

		// Use pod name as key and port as value
		data[parts[0]] = parts[1]
	}

	// Create ConfigMap
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            rf.Name + advertisedPortsConfigMapNameSuffix,
			Namespace:       rf.Namespace,
			Labels:          labels,
			OwnerReferences: ownerRefs,
		},
		Data: data,
	}
}

// decorateRedisStatefulSet adds the necessary configurations for advertised ports
func decorateRedisStatefulSet(rf *redisfailoverv1.RedisFailover, sts *appsv1.StatefulSet) *appsv1.StatefulSet {
	// Find the Redis container
	var redisContainerIndex int = -1
	for i, c := range sts.Spec.Template.Spec.Containers {
		if c.Name == "redis" {
			redisContainerIndex = i
			break
		}
	}

	// If Redis container not found, return unmodified StatefulSet
	if redisContainerIndex == -1 {
		return sts
	}

	// Modify the Redis container directly in the StatefulSet
	// append custom command "--replica-announce-ip" "" "--replica-announce-port" "$(cat /redis/advertised-ports/$(hostname))"

	commands := append(sts.Spec.Template.Spec.Containers[redisContainerIndex].Command,
		"--replica-announce-ip", "$REDIS_REPLICA_ANNOUNCE_IP",
		"--replica-announce-port", "$(cat /redis/advertised-ports/$(hostname))",
	)
	sts.Spec.Template.Spec.Containers[redisContainerIndex].Command = []string{
		"sh", "-c",
		strings.Join(commands, " "),
	}
	// append custom env var REDIS_REPLICA_ANNOUNCE_IP which ref to status.hostIP
	sts.Spec.Template.Spec.Containers[redisContainerIndex].Env = append(
		sts.Spec.Template.Spec.Containers[redisContainerIndex].Env,
		corev1.EnvVar{
			Name: "REDIS_REPLICA_ANNOUNCE_IP",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "status.hostIP",
				},
			},
		},
	)

	// add volume which ref configmap
	sts.Spec.Template.Spec.Volumes = append(sts.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: "advertised-ports",
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: rf.Name + advertisedPortsConfigMapNameSuffix,
				},
			},
		},
	})

	// add container volumemount
	sts.Spec.Template.Spec.Containers[redisContainerIndex].VolumeMounts = append(
		sts.Spec.Template.Spec.Containers[redisContainerIndex].VolumeMounts,
		corev1.VolumeMount{
			Name:      "advertised-ports",
			MountPath: "/redis/advertised-ports",
		},
	)

	return sts
}

func (r *RedisFailoverKubeClientCustom) EnsureRedisMasterService(rf *redisfailoverv1.RedisFailover, labels map[string]string, ownerRefs []metav1.OwnerReference) error {
	if err := r.RedisFailoverKubeClient.EnsureRedisMasterService(rf, labels, ownerRefs); err != nil {
		return err
	}

	nodePortAnno, ok := rf.Annotations[AnnotationKeyAdvertisedPort]
	if !ok || nodePortAnno == "" {
		return nil
	}

	pairs := strings.Split(nodePortAnno, ",")
	redisPort := int32(6379)
	if rf.Spec.Redis.Port != 0 {
		redisPort = rf.Spec.Redis.Port
	}

	for _, pair := range pairs {
		parts := strings.Split(pair, ":")
		if len(parts) != 2 {
			continue
		}

		podName, nodePort := parts[0], parts[1]
		serviceName := podName + nodePortSuffix

		var nodePortInt int32
		if _, err := fmt.Sscanf(nodePort, "%d", &nodePortInt); err != nil || nodePortInt < 30000 || nodePortInt > 32767 {
			continue
		}

		selectorLabels := util.MergeLabels(
			generateSelectorLabels(redisRoleName, rf.Name),
			map[string]string{"statefulset.kubernetes.io/pod-name": podName},
		)

		existingSvc, err := r.K8SService.GetService(rf.Namespace, serviceName)
		if err == nil {
			needsUpdate := false
			if len(existingSvc.Spec.Ports) > 0 && existingSvc.Spec.Ports[0].NodePort != nodePortInt {
				needsUpdate = true
			} else if !reflect.DeepEqual(existingSvc.Spec.Selector, selectorLabels) {
				needsUpdate = true
			}

			if !needsUpdate {
				continue
			}
		}

		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:            serviceName,
				Namespace:       rf.Namespace,
				Labels:          labels,
				OwnerReferences: ownerRefs,
			},
			Spec: corev1.ServiceSpec{
				Type:     corev1.ServiceTypeNodePort,
				Selector: selectorLabels,
				Ports: []corev1.ServicePort{
					{
						Name:       "redis",
						Port:       redisPort,
						TargetPort: intstr.FromInt(int(redisPort)),
						NodePort:   nodePortInt,
						Protocol:   corev1.ProtocolTCP,
					},
				},
			},
		}

		if err := r.K8SService.CreateOrUpdateService(rf.Namespace, svc); err != nil {
			r.setEnsureOperationMetrics(svc.Namespace, svc.Name, "Service", rf.Name, err)
			return err
		}
		r.setEnsureOperationMetrics(svc.Namespace, svc.Name, "Service", rf.Name, nil)
	}

	return nil
}
