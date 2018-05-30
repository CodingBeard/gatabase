package gataerrors

import (
	"testing"
	"errors"
)

func TestNewGataError(t *testing.T) {
	underlying := errors.New("underlying error")
	gataerror := NewGataError("message string", underlying)

	if gataerror.Message != "message string" {
		t.Error("failed to inject Message into the gataerror")
	}

	if gataerror.Underlying != underlying {
		t.Error("failed to inject Underlying error into the gataerror")
	}
}

func TestGataError_SetUnderlying(t *testing.T) {
	underlying := errors.New("underlying error")

	gataerror := GataError{Message:"message string"}

	gataerror.SetUnderlying(underlying)

	if gataerror.Underlying != underlying {
		t.Error("failed to inject Underlying error into the gataerror")
	}
}

func TestGataError_Error(t *testing.T) {
	underlying := errors.New("underlying error")
	gataerror := NewGataError("message string", underlying)

	errorString := "message string\nUnderlying: underlying error"

	if gataerror.Error() != errorString {
		t.Error("failed to generate correct error message")
	}
}
