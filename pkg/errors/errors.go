package errors

func IsRetryable(err error) bool {
	return false
}
