package manager

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"syscall"
	"time"

	"github.com/kube-vip/kube-vip/pkg/cluster"
	"github.com/kube-vip/kube-vip/pkg/iptables"
	"github.com/kube-vip/kube-vip/pkg/vip"
	log "github.com/sirupsen/logrus"
	"github.com/vishvananda/netlink"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// Start will begin the Manager, which will start services and watch the configmap
func (sm *Manager) startTableMode(id string) error {
	var cpCluster *cluster.Cluster
	var err error

	// use a Go context so we can tell the leaderelection code when we
	// want to step down
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log.Infof("all routing table entries will exist in table [%d] with protocol [%d]", sm.config.RoutingTableID, sm.config.RoutingProtocol)

	if sm.config.CleanRoutingTable {
		go func() {
			// we assume that after 10s all services should be configured so we can delete redundant routes
			time.Sleep(time.Second * 10)
			if err := sm.cleanRoutes(); err != nil {
				log.Errorf("error checking for old routes: %v", err)
			}
		}()
	}

	egressCleanEnv := os.Getenv("EGRESS_CLEAN")
	if egressCleanEnv != "" {
		egressClean, err := strconv.ParseBool(egressCleanEnv)
		if err != nil {
			log.Warnf("failed to parse EGRESS_CLEAN env value [%s]. Egress cleaning will not be performed: %s", egressCleanEnv, err.Error())
		}
		if egressClean {
			vip.ClearIPTables(sm.config.EgressWithNftables, sm.config.ServiceNamespace, iptables.ProtocolIPv4)
			vip.ClearIPTables(sm.config.EgressWithNftables, sm.config.ServiceNamespace, iptables.ProtocolIPv6)
			log.Debug("IPTables rules cleaned on startup")
		}
	}

	// Shutdown function that will wait on this signal, unless we call it ourselves
	go func() {
		<-sm.signalChan
		log.Info("Received kube-vip termination, signaling shutdown")
		if sm.config.EnableControlPlane {
			cpCluster.Stop()
		}

		// Cancel the context, which will in turn cancel the leadership
		cancel()
	}()

	if sm.config.EnableControlPlane {
		cpCluster, err = cluster.InitCluster(sm.config, false)
		if err != nil {
			return fmt.Errorf("cluster initialization error: %w", err)
		}

		clusterManager, err := initClusterManager(sm)
		if err != nil {
			return fmt.Errorf("cluster manager initialization error: %w", err)
		}

		if err := cpCluster.StartVipService(sm.config, clusterManager, nil, nil); err != nil {
			log.Errorf("Control Plane Error [%v]", err)
			// Trigger the shutdown of this manager instance
			sm.signalChan <- syscall.SIGINT
		}
	} else {
		ns, err := returnNameSpace()
		if err != nil {
			log.Warnf("unable to auto-detect namespace, dropping to [%s]", sm.config.Namespace)
			ns = sm.config.Namespace
		}

		// Start a services watcher (all kube-vip pods will watch services), upon a new service
		// a lock based upon that service is created that they will all leaderElection on
		if sm.config.EnableServicesElection {
			log.Infof("beginning watching services, leaderelection will happen for every service")
			err = sm.startServicesWatchForLeaderElection(ctx)
			if err != nil {
				return err
			}
		} else if sm.config.EnableLeaderElection {

			log.Infof("beginning services leadership, namespace [%s], lock name [%s], id [%s]", ns, plunderLock, id)
			// we use the Lease lock type since edits to Leases are less common
			// and fewer objects in the cluster watch "all Leases".
			lock := &resourcelock.LeaseLock{
				LeaseMeta: metav1.ObjectMeta{
					Name:      plunderLock,
					Namespace: ns,
				},
				Client: sm.clientSet.CoordinationV1(),
				LockConfig: resourcelock.ResourceLockConfig{
					Identity: id,
				},
			}
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
						err = sm.servicesWatcher(ctx, sm.syncServices)
						if err != nil {
							log.Fatal(err)
						}
					},
					OnStoppedLeading: func() {
						// we can do cleanup here
						log.Infof("leader lost: %s", id)
						for _, instance := range sm.serviceInstances {
							for _, cluster := range instance.clusters {
								cluster.Stop()
							}
						}

						log.Fatal("lost leadership, restarting kube-vip")
					},
					OnNewLeader: func(identity string) {
						// we're notified when new leader elected
						if identity == id {
							// I just got the lock
							return
						}
						log.Infof("new leader elected: %s", identity)
					},
				},
			})
		} else {
			log.Infof("beginning watching services without leader election")
			err = sm.servicesWatcher(ctx, sm.syncServices)
			if err != nil {
				log.Errorf("Cannot watch services, %v", err)
			}
		}
	}

	return nil
}

func (sm *Manager) cleanRoutes() error {
	routes, err := vip.ListRoutes(sm.config.RoutingTableID, sm.config.RoutingProtocol)
	if err != nil {
		return fmt.Errorf("error getting routes: %w", err)
	}

	for i := range routes {
		found := false
		if sm.config.EnableControlPlane {
			found = (routes[i].Dst.IP.String() == sm.config.Address)
		} else {
			for _, instance := range sm.serviceInstances {
				for _, cluster := range instance.clusters {
					for n := range cluster.Network {
						r := cluster.Network[n].PrepareRoute()
						if r.Dst.String() == routes[i].Dst.String() {
							found = true
						}
					}
				}
			}
		}

		if !found {
			err = netlink.RouteDel(&(routes[i]))
			if err != nil {
				log.Errorf("[route] error deleting route: %v", routes[i])
			}
			log.Debugf("[route] deleted route: %v", routes[i])
		}

	}
	return nil
}

func (sm *Manager) countRouteReferences(route *netlink.Route) int {
	cnt := 0
	for _, instance := range sm.serviceInstances {
		for _, cluster := range instance.clusters {
			for n := range cluster.Network {
				r := cluster.Network[n].PrepareRoute()
				if r.Dst.String() == route.Dst.String() {
					cnt++
				}
			}
		}
	}
	return cnt
}
