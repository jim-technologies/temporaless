package workflow

import (
	"errors"
	"fmt"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
)

// ErrUserPanic identifies a panic raised by a user workflow, activity, or
// AllActivities branch callback. Temporaless converts those panics into normal
// errors so one malformed handler cannot terminate the worker process.
var ErrUserPanic = errors.New("user callback panicked")

// UserPanicError is the typed error produced when a user callback panics.
// PanicValue contains the safely formatted recovered value on the original
// invocation. Message is populated when reconstructing the error from a
// durable record, where the original Go value is intentionally unavailable.
type UserPanicError struct {
	Boundary   string
	PanicValue string
	Message    string
}

func (err *UserPanicError) Error() string {
	if err.Message != "" {
		return err.Message
	}
	boundary := err.Boundary
	if boundary == "" {
		boundary = "user"
	}
	return fmt.Sprintf("%s callback panicked: %s", boundary, err.PanicValue)
}

func (err *UserPanicError) Is(target error) bool {
	return target == ErrUserPanic
}

func invokeUser[T any](boundary string, execute func() (T, error)) (result T, err error) {
	defer func() {
		recovered := recover()
		if recovered == nil {
			return
		}
		var zero T
		result = zero
		err = &UserPanicError{
			Boundary:   boundary,
			PanicValue: formatPanicValue(recovered),
		}
	}()
	return execute()
}

func invokeUserConstructor[T any](boundary string, construct func() T) (T, error) {
	return invokeUser(boundary, func() (T, error) { return construct(), nil })
}

// durableUserPanicError gives a committed workflow panic the same public error
// shape both when it is first persisted and when the terminal record replays.
// An activity panic is already an ActivityError with a UserPanicError cause;
// preserve that error instead of adding a second ActivityError layer.
func durableUserPanicError(failure *temporalessv1.ActivityFailure, original error) error {
	if original != nil {
		var activityErr *ActivityError
		if errors.As(original, &activityErr) &&
			activityErr.Code == failure.GetCode() {
			var panicErr *UserPanicError
			if errors.As(activityErr, &panicErr) {
				return activityErr
			}
		}

		var panicErr *UserPanicError
		if errors.As(original, &panicErr) {
			return &ActivityError{
				Code:    failure.GetCode(),
				Message: failure.GetMessage(),
				Cause:   panicErr,
			}
		}
	}

	return &ActivityError{
		Code:    failure.GetCode(),
		Message: failure.GetMessage(),
		Cause:   &UserPanicError{Message: failure.GetMessage()},
	}
}

func formatPanicValue(value any) (formatted string) {
	formatted = fmt.Sprintf("%T", value)
	defer func() {
		if recover() != nil {
			// Keep the type-only fallback assigned above. A hostile String or
			// Error method must not turn panic containment into another panic.
			return
		}
	}()
	return fmt.Sprint(value)
}
