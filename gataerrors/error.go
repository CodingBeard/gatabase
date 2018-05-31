package gataerrors

type GataError struct {
	Message    string
	Underlying error
}

func NewGataError(message string) *GataError {
	return &GataError{Message: message}
}

// Echo the current error and the previous error
func (error *GataError) Error() string {
	message := error.Message

	if len(error.Underlying.Error()) > 0 {
		message += "\nUnderlying: " + error.Underlying.Error()
	}

	return message
}

// Fluent setter
func (error *GataError) SetUnderlying(previous error) *GataError {
	error.Underlying = previous

	return error
}

// Compare the current error message
func (error *GataError) IsSame(compare error) bool {
	gataError, isGata := compare.(*GataError)

	if isGata {
		if error.Message == gataError.Message {
			return true
		} else {
			return false
		}
	} else if error.Message == compare.Error() {
		return true
	}

	return false
}
