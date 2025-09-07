package utils

import "github.com/jackc/pglogrepl"

type NullableLSN struct {
	Valid bool
	LSN   pglogrepl.LSN
}

func (dst *NullableLSN) Scan(src any) error {
	if src == nil {
		*dst = NullableLSN{}
		return nil
	}

	var lsn pglogrepl.LSN
	if err := lsn.Scan(src); err != nil {
		return err
	}

	*dst = NullableLSN{Valid: true, LSN: lsn}

	return nil
}
