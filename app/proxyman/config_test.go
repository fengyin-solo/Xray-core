package proxyman

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

func TestSniffingConfigProtoRoundtrip(t *testing.T) {
	original := &SniffingConfig{
		Enabled:         true,
		SniffingTimeout: 350,
		MaxAttempts:     4,
	}
	b, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("failed to marshal SniffingConfig: %v", err)
	}
	var decoded SniffingConfig
	if err := proto.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("failed to unmarshal SniffingConfig: %v", err)
	}
	if decoded.GetSniffingTimeout() != 350 || decoded.GetMaxAttempts() != 4 {
		t.Fatalf("sniffing timing fields lost on the wire: timeout=%d attempts=%d", decoded.GetSniffingTimeout(), decoded.GetMaxAttempts())
	}

	fields := File_app_proxyman_config_proto.Messages().ByName("SniffingConfig").Fields()
	timeoutField := fields.ByName("sniffing_timeout")
	attemptsField := fields.ByName("max_attempts")
	if timeoutField.Number() != 7 || attemptsField.Number() != 8 {
		t.Fatalf("descriptor has wrong field numbers: timeout=%d attempts=%d", timeoutField.Number(), attemptsField.Number())
	}
}

func TestBuildSniffingRequestTiming(t *testing.T) {
	req, err := BuildSniffingRequest(&SniffingConfig{SniffingTimeout: 250, MaxAttempts: 3})
	if err != nil {
		t.Fatalf("BuildSniffingRequest failed: %v", err)
	}
	if req.SniffingTimeout != 250*time.Millisecond {
		t.Fatalf("unexpected sniffing timeout: %v", req.SniffingTimeout)
	}
	if req.MaxAttempts != 3 {
		t.Fatalf("unexpected max attempts: %d", req.MaxAttempts)
	}

	// Unset values stay zero so the dispatcher can apply its built-in defaults.
	defaultReq, err := BuildSniffingRequest(nil)
	if err != nil {
		t.Fatalf("BuildSniffingRequest(nil) failed: %v", err)
	}
	if defaultReq.SniffingTimeout != 0 || defaultReq.MaxAttempts != 0 {
		t.Fatalf("expected zero-valued defaults, got: %+v", defaultReq)
	}
}
