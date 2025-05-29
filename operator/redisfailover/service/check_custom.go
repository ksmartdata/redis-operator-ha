package service

import (
	"fmt"
	"strings"

	redisfailoverv1 "github.com/spotahome/redis-operator/api/redisfailover/v1"
	"github.com/spotahome/redis-operator/log"
	"github.com/spotahome/redis-operator/metrics"
	"github.com/spotahome/redis-operator/service/k8s"
	"github.com/spotahome/redis-operator/service/redis"
)

// RedisFailoverChecker is our implementation of RedisFailoverCheck interface
type RedisFailoverCheckerCustom struct {
	RedisFailoverChecker
}

// NewRedisFailoverChecker creates an object of the RedisFailoverChecker struct
func NewRedisFailoverCheckerCustom(k8sService k8s.Services, redisClient redis.Client, logger log.Logger, metricsClient metrics.Recorder) *RedisFailoverCheckerCustom {
	logger = logger.With("service", "redis.checkercustom")
	return &RedisFailoverCheckerCustom{
		RedisFailoverChecker: RedisFailoverChecker{
			k8sService:    k8sService,
			redisClient:   redisClient,
			logger:        logger,
			metricsClient: metricsClient,
		},
	}
}

func (r *RedisFailoverCheckerCustom) CheckAllSlavesFromMaster(master string, rf *redisfailoverv1.RedisFailover) error {
	if _, ok := rf.Annotations[AnnotationKeyAdvertisedPort]; !ok {
		return r.RedisFailoverChecker.CheckAllSlavesFromMaster(master, rf)
	}

	rps, err := r.k8sService.GetStatefulSetPods(rf.Namespace, GetRedisName(rf))
	if err != nil {
		return err
	}

	password, err := k8s.GetRedisPassword(r.k8sService, rf)
	if err != nil {
		return err
	}

	// master is pod ip of redis master
	// we should get the pod's host ip of the pod
	// then check if the slave is the host ip
	masterHostIP := ""
	for _, rp := range rps.Items {
		if rp.Status.PodIP == master {
			// overwrite
			masterHostIP = rp.Status.HostIP
			break
		}
	}

	rport := getRedisPort(rf.Spec.Redis.Port)
	for _, rp := range rps.Items {
		if rp.Status.PodIP == master {
			err = r.setMasterLabelIfNecessary(rf.Namespace, rp)
			if err != nil {
				return err
			}
		} else {
			err = r.setSlaveLabelIfNecessary(rf.Namespace, rp)
			if err != nil {
				return err
			}
		}

		realMaster, err := r.redisClient.GetSlaveOf(rp.Status.PodIP, rport, password)
		if err != nil {
			r.logger.Errorf("Get slave of master failed, maybe this node is not ready, pod ip: %s", rp.Status.PodIP)
			return err
		}
		if realMaster != "" && realMaster != masterHostIP {
			return fmt.Errorf("slave %s don't have the master %s, has %s", rp.Status.PodIP, masterHostIP, realMaster)
		}
	}
	return nil
}

func (r *RedisFailoverCheckerCustom) CheckSentinelMonitor(sentinel string, monitor ...string) error {
	if len(monitor) < 4 || monitor[2] == "" {
		return r.RedisFailoverChecker.CheckSentinelMonitor(sentinel, monitor...)
	}

	monitorIP := monitor[0]
	monitorPort := monitor[1]
	ns := monitor[3]

	// parse the monitor[2] to get the pod name and port
	// monitor[2] is like "rfr-redis-0:32220,rfr-redis-1:32221,rfr-redis-2:32222"
	pods := make(map[string]string)
	pairs := strings.Split(monitor[2], ",")
	if len(pairs) == 0 {
		return fmt.Errorf("invalid annotation value: %s", monitor[2])
	}
	for _, pair := range pairs {
		parts := strings.Split(pair, ":")
		if len(parts) != 2 {
			return fmt.Errorf("invalid annotation pair format: %s", pair)
		}
		pods[parts[0]] = parts[1]
	}
	for podName, port := range pods {
		pod, err := r.k8sService.GetPod(ns, podName)
		if err != nil {
			return err
		}
		if pod.Status.PodIP == monitorIP {
			// overwrite
			monitorIP = pod.Status.HostIP
			monitorPort = port
			break
		}
	}

	actualMonitorIP, actualMonitorPort, err := r.redisClient.GetSentinelMonitor(sentinel)
	if err != nil {
		return err
	}
	if actualMonitorIP != monitorIP || (monitorPort != "" && monitorPort != actualMonitorPort) {
		return fmt.Errorf("sentinel monitoring %s:%s instead %s:%s", actualMonitorIP, actualMonitorPort, monitorIP, monitorPort)
	}
	return nil

}
