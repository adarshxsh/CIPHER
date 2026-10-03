package model

import "time"

type DemandMetadata struct {
	FileID       string
	Demand       int
	ReplicaCount int
	Timestamp    time.Time
	Version      uint64
}