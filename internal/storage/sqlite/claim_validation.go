package sqlite

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

func encodeRegistrations(registrations []storage.FunctionRegistration) ([]byte, error) {
	if len(registrations) == 0 {
		return nil, nil
	}

	values := make(map[string]string, len(registrations))
	for _, registration := range registrations {
		if registration.Key == "" || registration.Signature == "" {
			return nil, errors.New("claim ready node: function registration is incomplete")
		}

		if _, exists := values[registration.Key]; exists {
			return nil, fmt.Errorf("claim ready node: duplicate function registration %q", registration.Key)
		}

		values[registration.Key] = registration.Signature
	}

	encoded, err := json.Marshal(values)
	if err != nil {
		return nil, fmt.Errorf("claim ready node: encode function registrations: %w", err)
	}

	return encoded, nil
}

func validateClaimLease(owner string, leaseTTL time.Duration) error {
	if owner == "" {
		return errors.New("claim ready node: lease owner is empty")
	}

	if leaseTTL <= 0 {
		return errors.New("claim ready node: lease TTL must be positive")
	}

	return nil
}
