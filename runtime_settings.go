package cord

import (
	"errors"
	"runtime"
	"time"
)

func runtimeSettings(options Options) (schedulerSettings, error) {
	concurrency := options.Concurrency
	if concurrency == 0 {
		concurrency = max(1, runtime.GOMAXPROCS(0))
	}

	pollInterval := options.PollInterval
	if pollInterval == 0 {
		pollInterval = defaultPollInterval
	}

	leaseTTL := options.LeaseTTL
	if leaseTTL == 0 {
		leaseTTL = defaultLeaseTTL
	}

	heartbeatInterval := options.HeartbeatInterval
	if heartbeatInterval == 0 {
		heartbeatInterval = max(
			minimumLeasePrecision,
			min(defaultHeartbeatInterval, leaseTTL/heartbeatsPerLease),
		)
	}

	retry, err := retrySettings(options)
	if err != nil {
		return schedulerSettings{}, err
	}

	if err := validateSchedulerSettings(concurrency, pollInterval, leaseTTL, heartbeatInterval); err != nil {
		return schedulerSettings{}, err
	}

	return schedulerSettings{
		concurrency:       concurrency,
		pollInterval:      pollInterval,
		leaseTTL:          leaseTTL,
		heartbeatInterval: heartbeatInterval,
		onSchedulerError:  options.OnSchedulerError,
		retry:             retry,
	}, nil
}

func validateSchedulerSettings(
	concurrency int,
	pollInterval time.Duration,
	leaseTTL time.Duration,
	heartbeatInterval time.Duration,
) error {
	if concurrency < 1 {
		return errors.New("cord: concurrency must be positive")
	}

	if pollInterval <= 0 {
		return errors.New("cord: poll interval must be positive")
	}

	if leaseTTL <= minimumLeaseTTL {
		return errors.New("cord: lease TTL must be greater than two milliseconds")
	}

	if heartbeatInterval < minimumLeasePrecision || heartbeatInterval >= leaseTTL-heartbeatInterval {
		return errors.New("cord: heartbeat interval must be at least one millisecond and less than half the lease TTL")
	}

	return nil
}

func retrySettings(options Options) (retryPolicy, error) {
	policy := defaultRetryPolicy()
	if options.MaxAttempts != 0 {
		policy.maxAttempts = options.MaxAttempts
	}

	if options.RetryBaseDelay != 0 {
		policy.baseDelay = options.RetryBaseDelay
	}

	if options.RetryMaxDelay != 0 {
		policy.maxDelay = options.RetryMaxDelay
	}

	if err := policy.validate(); err != nil {
		return retryPolicy{}, err
	}

	return policy, nil
}
