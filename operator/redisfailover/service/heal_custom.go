package service

import (
	"fmt"
	"strconv"
	"strings"

	redisfailoverv1 "github.com/spotahome/redis-operator/api/redisfailover/v1"
	"github.com/spotahome/redis-operator/log"
	"github.com/spotahome/redis-operator/service/k8s"
	"github.com/spotahome/redis-operator/service/redis"
)

const (
	// AnnotationKeyAdvertisedPort value format "rfr-redis-0:22220,rfr-redis-1:22221,rfr-redis-2:22222"
	AnnotationKeyAdvertisedPort = "redis-operator.spotahome.com/redis-advertised-port"
)

// RedisFailoverHealer is our implementation of RedisFailoverCheck interface
type RedisFailoverHealerCustom struct {
	RedisFailoverHealer
}

// NewRedisFailoverHealer creates an object of the RedisFailoverChecker struct
func NewRedisFailoverHealerCustom(k8sService k8s.Services, redisClient redis.Client, logger log.Logger) *RedisFailoverHealerCustom {
	logger = logger.With("service", "redis.healercustom")
	return &RedisFailoverHealerCustom{
		RedisFailoverHealer: RedisFailoverHealer{
			k8sService:  k8sService,
			redisClient: redisClient,
			logger:      logger,
		},
	}
}

// NewSentinelMonitor sentinel monitor ${monitor} ${rf.Spec.Redis.Port} ${rf.Spec.Sentinel.Replicas/2 + 1}
func (r *RedisFailoverHealerCustom) NewSentinelMonitor(ip string, monitor string, rf *redisfailoverv1.RedisFailover) error {
	if v, ok := rf.Annotations[AnnotationKeyAdvertisedPort]; ok {
		var err error
		var port string
		monitor, port, err = r.getAdvertisedIpPort(rf.Namespace, monitor, v)
		if err != nil {
			return err
		}

		quorum := strconv.Itoa(int(getQuorum(rf)))
		password, err := k8s.GetRedisPassword(r.k8sService, rf)
		if err != nil {
			return err
		}
		return r.redisClient.MonitorRedisWithPort(ip, monitor, port, quorum, password)
	}

	return r.RedisFailoverHealer.NewSentinelMonitor(ip, monitor, rf)
}

// NewSentinelMonitorWithPort sentinel monitor ${monitor} ${port} ${rf.Spec.Sentinel.Replicas/2 + 1}
func (r *RedisFailoverHealerCustom) NewSentinelMonitorWithPort(ip string, monitor string, port string, rf *redisfailoverv1.RedisFailover) error {
	if v, ok := rf.Annotations[AnnotationKeyAdvertisedPort]; ok {
		var err error
		monitor, port, err = r.getAdvertisedIpPort(rf.Namespace, monitor, v)
		if err != nil {
			return err
		}
	}
	return r.RedisFailoverHealer.NewSentinelMonitorWithPort(ip, monitor, port, rf)
}

func (r *RedisFailoverHealerCustom) getAdvertisedIpPort(ns, ip string, annoVal string) (string, string, error) {
	// Parse annotation value
	// Format: "rfr-redis-0:32220,rfr-redis-1:32221,rfr-redis-2:32222"
	pairs := strings.Split(annoVal, ",")
	if len(pairs) == 0 {
		return "", "", fmt.Errorf("invalid annotation value: %s", annoVal)
	}

	// Create a map for quick lookup of pod name -> port
	podPortMap := make(map[string]string, len(pairs))
	for _, pair := range pairs {
		parts := strings.Split(pair, ":")
		if len(parts) != 2 {
			r.logger.Warningf("Invalid annotation pair format: %s", pair)
			continue
		}
		podPortMap[parts[0]] = parts[1]
	}

	// Iterate through all possible pod names until finding the matching IP
	for podName, port := range podPortMap {
		pod, err := r.k8sService.GetPod(ns, podName)
		if err != nil {
			r.logger.Debugf("Failed to get pod %s: %v", podName, err)
			continue
		}
		if pod.Status.PodIP == ip {
			// Found the pod with matching IP
			return pod.Status.HostIP, port, nil
		}
	}

	return "", "", fmt.Errorf("pod with ip %s not found", ip)
}
