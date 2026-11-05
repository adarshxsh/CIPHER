package model

type ProviderState string

const (
	Active  ProviderState = "ACTIVE"
	Removed ProviderState = "REMOVED"
)

type Replica struct {
    ReplicaID  string
    ProviderID string
    FileID     string
    Version    uint64
    State      ProviderState
}