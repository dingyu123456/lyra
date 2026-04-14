package framework

// SharedLister groups scheduler-specific listers.
type SharedLister interface {
	ClusterInfos() ClusterInfoLister
}

// ClusterInfoLister interface represents anything that can list/get ClusterInfo objects from cluster name.
type ClusterInfoLister interface {
	// List returns the list of ClusterInfos.
	List() ([]*ClusterInfo, error)
	// Get returns the ClusterInfo of the given cluster name.
	Get(clusterName string) (*ClusterInfo, error)
}
