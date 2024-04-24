package manager

import (
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// The startServicesWatchForLeaderElection function will start a services watcher, the
func (sm *Manager) startServicesWatchForLeaderElection(ctx context.Context) error {
	err := sm.servicesWatcher(ctx, sm.StartServicesLeaderElection)
	if err != nil {
		return err
	}

	for _, instance := range sm.serviceInstances {
		for _, cluster := range instance.clusters {
			for i := range cluster.Network {
				_ = cluster.Network[i].DeleteRoute()
			}
			cluster.Stop()
		}
	}

	log.Infof("Shutting down kube-Vip")

	return nil
}

// used to get a list of nodes where there is a healthy pods for this service running.
func findNodesWithHealthyPods(ctx context.Context, clientset *kubernetes.Clientset, service *v1.Service) ([]string, error) {
	selector := labels.Set(service.Spec.Selector).String()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	// Retry until pods are found or context deadline exceeds
	for {
		podList, err := clientset.CoreV1().Pods(service.Namespace).List(ctx,
			metav1.ListOptions{
				LabelSelector: selector,
			})
		if err != nil {
			fmt.Println("Error fetching pods, retrying:", err)
		} else if len(podList.Items) == 0 {
			fmt.Println("No pods found, retrying...")
			time.Sleep(5 * time.Second)
		} else {
			log.Infof("the returned pods are %v", podList.Items)

			var healthyPods []string

			// Iterate through the list of pods
			for _, pod := range podList.Items {
				for _, condition := range pod.Status.Conditions {
					if condition.Type == v1.PodReady && condition.Status == v1.ConditionTrue {
						healthyPods = append(healthyPods, pod.Spec.NodeName)
					} else {
						log.Infof("Ignoring a pod %v and %v ", condition.Type, condition.Status)
					}
				}
			}

			if len(healthyPods) == 0 {
				return nil, fmt.Errorf("no healthy pods found")
			}

			return healthyPods, nil // Return the list of all healthy pods
		}
	}
}

// The responsibility of this function is to become the leader for a service if the current
// leader is not running on the same node as one of the services pods
func (sm *Manager) acquireLeastForThisNodeIfLocalNode(ctx context.Context, service *v1.Service, currentLeader string) {
	nodesWithHealthPod, err := findNodesWithHealthyPods(ctx, sm.clientSet, service)

	if err != nil {
		log.Errorf("Error finding nodes with healthy pods: %v", err)
		return
	} else {
		log.Debugf("The nodes with healthy pods are: %v", nodesWithHealthPod)
	}

	// check if the current lease-holder is running on a node with a healthy pod.
	// if true then exit.
	for _, p := range nodesWithHealthPod {
		if p == currentLeader {
			log.Debugf("exiting becuase the current lease holder is a healthy local pod")
			return
		}
	}

	if nodesWithHealthPod[0] == sm.config.NodeName {
		log.Infof("Preferred node is healthy, taking over leadership forcefully.")
		lease, err := sm.clientSet.CoordinationV1().Leases(service.Namespace).Get(ctx, fmt.Sprintf("kubevip-%s", service.Name),
			metav1.GetOptions{})
		if err != nil {
			log.Errorln(err)
			return
		}
		// things have changed since you were notified. stops a race condition where many nodes are force updating
		if *lease.Spec.HolderIdentity != currentLeader {
			log.Infof("We are existing because the lease has changed recently from  %v to %v",
				currentLeader, *lease.Spec.HolderIdentity)
			return
		}
		lease.Spec.HolderIdentity = &sm.config.NodeName
		lease, err = sm.clientSet.CoordinationV1().Leases(service.Namespace).Update(ctx, lease,
			metav1.UpdateOptions{})
		if err != nil {
			log.Errorln(err)
			return
		}
		log.Infof("(svc election) the lease has been acquired")
	} else {
		log.Infof("(svc election) new leader elected and no action taken on this node: %s", currentLeader)
	}

}

// The startServicesWatchForLeaderElection function will start a services watcher, the
func (sm *Manager) StartServicesLeaderElection(ctx context.Context, service *v1.Service, wg *sync.WaitGroup) error {
	serviceLease := fmt.Sprintf("kubevip-%s", service.Name)
	log.Infof("(svc election) service [%s], namespace [%s], lock name [%s], host id [%s]", service.Name, service.Namespace, serviceLease, sm.config.NodeName)
	// we use the Lease lock type since edits to Leases are less common
	// and fewer objects in the cluster watch "all Leases".
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      serviceLease,
			Namespace: service.Namespace,
		},
		Client: sm.clientSet.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: sm.config.NodeName,
		},
	}

	activeService[string(service.UID)] = true
	// start the leader election code loop
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock: lock,
		// IMPORTANT: you MUST ensure that any code you have that
		// is protected by the lease must terminate **before**
		// you call cancel. Otherwise, you could have a background
		// loop still running and another process could
		// get elected before your background loop finished, violating
		// the stated goal of the lease.
		ReleaseOnCancel: true,
		LeaseDuration:   time.Duration(sm.config.LeaseDuration) * time.Second,
		RenewDeadline:   time.Duration(sm.config.RenewDeadline) * time.Second,
		RetryPeriod:     time.Duration(sm.config.RetryPeriod) * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				// Mark this service as active (as we've started leading)
				// we run this in background as it's blocking
				wg.Add(1)
				go func() {
					if err := sm.syncServices(ctx, service, wg); err != nil {
						log.Errorln(err)
					}
				}()
			},
			OnStoppedLeading: func() {
				// we can do cleanup here
				log.Infof("(svc election) service [%s] leader lost: [%s]", service.Name, sm.config.NodeName)
				if activeService[string(service.UID)] {
					if err := sm.deleteService(string(service.UID)); err != nil {
						log.Errorln(err)
					}
				}
				// Mark this service is inactive
				activeService[string(service.UID)] = false
			},
			OnNewLeader: func(identity string) {
				// we're notified when new leader elected
				if identity == sm.config.NodeName {
					// I just got the lock
					return
				}
				prefersLocal, err := strconv.ParseBool(service.Labels["prefersLocal"])

				if err != nil {
					prefersLocal = false
				}
				if prefersLocal {
					sm.acquireLeastForThisNodeIfLocalNode(ctx, service, identity)
				} else {
					log.Infof("(svc election) new leader elected: %s", identity)
				}
			},
		},
	})
	log.Infof("(svc election) for service [%s] stopping", service.Name)
	return nil
}
