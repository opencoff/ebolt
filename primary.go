// primary.go - primary writer for replicas

package ebolt

type replica struct {
	*ReplicaOptions
}

type primary struct {
	*PrimaryOptions
}

func newPrimary(dn string, opt *PrimaryOptions) (*primary, error) {
	return nil, nil
}

func newReplica(dn string, opt *ReplicaOptions) (*replica, error) {
	return nil, nil
}

func (r *primary) Close() error {
	return nil
}

func (r *replica) Close() error {
	return nil
}
