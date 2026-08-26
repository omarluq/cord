package storage

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

const maxIdempotencyKeyBytes = 255

func validateSubmissionIdentity(run *Run) error {
	if run.IdempotencyKey == nil {
		if run.SubmissionFingerprint != nil {
			return errors.New("validate run plan: unkeyed run has a submission fingerprint")
		}

		return nil
	}

	key := *run.IdempotencyKey
	if key == "" {
		return errors.New("validate run plan: idempotency key is empty")
	}

	if !utf8.ValidString(key) {
		return errors.New("validate run plan: idempotency key is not valid UTF-8")
	}

	if strings.IndexByte(key, 0) >= 0 {
		return errors.New("validate run plan: idempotency key contains NUL")
	}

	if len(key) > maxIdempotencyKeyBytes {
		return fmt.Errorf(
			"validate run plan: idempotency key is longer than %d bytes",
			maxIdempotencyKeyBytes,
		)
	}

	if run.SubmissionFingerprint == nil || *run.SubmissionFingerprint == "" {
		return errors.New("validate run plan: keyed run submission fingerprint is empty")
	}

	return nil
}
