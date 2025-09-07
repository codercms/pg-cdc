package replication

import (
	"fmt"

	"github.com/jackc/pglogrepl"
)

type Error interface {
	replicationError()
	Error() string
}

type errorImpl struct{}

func (e *errorImpl) replicationError() {}

type MissingReplicationSlotError struct {
	errorImpl
}

func (e *MissingReplicationSlotError) Error() string {
	return "replication slot is missing"
}

type OverCompactedReplicationSlotError struct {
	errorImpl

	RequestedLSN pglogrepl.LSN
	AvailableLSN pglogrepl.LSN
}

func (e *OverCompactedReplicationSlotError) Error() string {
	return fmt.Sprintf(
		"slot overcompacted. Requested LSN %s but only LSNs >= %s are available",
		e.RequestedLSN.String(),
		e.AvailableLSN.String(),
	)
}

type ReplicationSlotAlreadyExistsError struct {
	errorImpl
}

func (e *ReplicationSlotAlreadyExistsError) Error() string {
	return "replication slot already exists"
}

type UnknownReplicationMessageError struct {
	errorImpl

	Type string
}

func (e *UnknownReplicationMessageError) Error() string {
	return "unexpected replication message: " + e.Type
}

type UnknownLogicalReplicationMessageError struct {
	errorImpl

	Type string
}

func (e *UnknownLogicalReplicationMessageError) Error() string {
	return "unexpected logical replication message: " + e.Type
}

type BareTransactionEventError struct {
	errorImpl
}

func (e *BareTransactionEventError) Error() string {
	return "received replication event outside of transaction"
}

type InvalidTransactionError struct {
	errorImpl
}

func (e *InvalidTransactionError) Error() string {
	return "lsn mismatch between BEGIN and COMMIT"
}

type NestedTransactionError struct {
	errorImpl
}

func (e *NestedTransactionError) Error() string {
	return "BEGIN within existing BEGIN stream"
}

type MissingPublicationError struct {
	errorImpl

	Publication string
}

func (e *MissingPublicationError) Error() string {
	return "publication " + e.Publication + " is missing"
}

type ReplicationMessageParseError struct {
	errorImpl

	Err error
}

func (e *ReplicationMessageParseError) Error() string {
	return "cannot parse replication message: " + e.Err.Error()
}
func (e *ReplicationMessageParseError) Unwrap() error {
	return e.Err
}

type LogicalReplicationMessageParseError struct {
	errorImpl

	Err error
}

func (e *LogicalReplicationMessageParseError) Error() string {
	return "cannot parse logical replication message: " + e.Err.Error()
}
func (e *LogicalReplicationMessageParseError) Unwrap() error {
	return e.Err
}
