package workflow

import (
	"errors"
	"fmt"

	"github.com/jim-technologies/temporaless/core/go/storage"
)

// ErrClaimCapabilityUnsupported is returned when workflow options request
// claim coordination but the configured store cannot atomically create a
// claim. The runtime rejects the option instead of silently degrading a
// requested single-flight guarantee to at-least-once execution.
var ErrClaimCapabilityUnsupported = errors.New("claim capability does not support requested coordination")

// ClaimCapabilityError identifies the option whose coordination requirement
// the configured claim store cannot satisfy.
type ClaimCapabilityError struct {
	Capability storage.ClaimCapability
	Option     string
}

func (err *ClaimCapabilityError) Error() string {
	return fmt.Sprintf(
		"claim capability %s does not support %s",
		err.Capability,
		err.Option,
	)
}

func (err *ClaimCapabilityError) Unwrap() error {
	return ErrClaimCapabilityUnsupported
}

func supportsCreateOnlyClaims(capability storage.ClaimCapability) bool {
	// CAS is reserved in the wire enum, but the current ClaimStore surface has
	// no fencing token, conditional refresh, conditional release, or takeover
	// operation. Treating CAS as usable here would advertise stronger
	// semantics than the runtime can enforce.
	return capability == storage.CreateOnlyClaims
}
