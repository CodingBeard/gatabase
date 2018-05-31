package gataerrors

import (
	"testing"
	"errors"
)

func TestNewGataError(t *testing.T) {
	gataerror := NewGataError("message string")

	if gataerror.Message != "message string" {
		t.Error("failed to inject Message into the gataerror")
	}
}

func TestGataError_SetUnderlying(t *testing.T) {
	underlying := errors.New("underlying error")

	gataerror := NewGataError("message string")

	gataerror.SetUnderlying(underlying)

	if gataerror.Underlying != underlying {
		t.Error("failed to inject Underlying error into the gataerror")
	}
}

func TestGataError_Error(t *testing.T) {
	underlying := errors.New("underlying error")
	gataerror := NewGataError("message string").SetUnderlying(underlying)

	errorString := "message string\nUnderlying: underlying error"

	if gataerror.Error() != errorString {
		t.Error("failed to generate correct error message")
	}
}

func TestGataError_IsSame(t *testing.T) {
	gataerror := NewGataError("message string").SetUnderlying(errors.New("underlying error"))

	if !gataerror.IsSame(errors.New("message string")) {
		t.Error("did not match errors when comparing error to GataError of same message")
	}

	if gataerror.IsSame(errors.New("mismatch")) {
		t.Error("got a match when comparing different errors")
	}

	gataerror2 := NewGataError("message string").SetUnderlying(errors.New("different underlying error"))

	if !gataerror.IsSame(gataerror2) {
		t.Error("did not match errors when comparing GataError to GataError of same message")
	}
}
