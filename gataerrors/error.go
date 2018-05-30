package gataerrors

type GataError struct {
	Message    string
	Underlying error
}

func NewGataError(message string, previous error) *GataError {
	return &GataError{Message: message, Underlying: previous}
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
func IsSameError(a, b error) bool {
	gataErrorA, isGataA := a.(*GataError)
	gataErrorB, isGataB := a.(*GataError)

	if isGataA && isGataB {
		if gataErrorA.Message == gataErrorB.Message {
			return true
		} else {
			return false
		}
	} else if isGataA && !isGataB {
		if gataErrorA.Message == b.Error() {
			return true
		} else {
			return false
		}
	} else if !isGataA && isGataB {
		if a.Error() == gataErrorB.Message {
			return true
		} else {
			return false
		}
	} else if a.Error() == b.Error() {
		return true
	}

	return false
}
