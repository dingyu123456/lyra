/*
Copyright 2024 The Lyra Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package framework

import (
	"errors"
	"testing"
)

func TestStatusCodes(t *testing.T) {
	tests := []struct {
		name     string
		code     Code
		expected string
	}{
		{"Success", Success, "Success"},
		{"Error", Error, "Error"},
		{"Unschedulable", Unschedulable, "Unschedulable"},
		{"UnschedulableAndUnresolvable", UnschedulableAndUnresolvable, "UnschedulableAndUnresolvable"},
		{"Wait", Wait, "Wait"},
		{"Skip", Skip, "Skip"},
		{"Pending", Pending, "Pending"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.code.String() != tt.expected {
				t.Errorf("Code.String() = %v, want %v", tt.code.String(), tt.expected)
			}
		})
	}
}

func TestNewStatus(t *testing.T) {
	tests := []struct {
		name     string
		code     Code
		reasons  []string
		wantCode Code
		wantMsg  string
	}{
		{
			name:     "success status",
			code:     Success,
			reasons:  []string{},
			wantCode: Success,
			wantMsg:  "",
		},
		{
			name:     "unschedulable with reason",
			code:     Unschedulable,
			reasons:  []string{"Insufficient CPU"},
			wantCode: Unschedulable,
			wantMsg:  "Insufficient CPU",
		},
		{
			name:     "unschedulable with multiple reasons",
			code:     Unschedulable,
			reasons:  []string{"Insufficient CPU", "Insufficient Memory"},
			wantCode: Unschedulable,
			wantMsg:  "Insufficient CPU, Insufficient Memory",
		},
		{
			name:     "error status",
			code:     Error,
			reasons:  []string{"Plugin error"},
			wantCode: Error,
			wantMsg:  "Plugin error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := NewStatus(tt.code, tt.reasons...)

			if status.Code() != tt.wantCode {
				t.Errorf("Status.Code() = %v, want %v", status.Code(), tt.wantCode)
			}

			if status.Message() != tt.wantMsg {
				t.Errorf("Status.Message() = %v, want %v", status.Message(), tt.wantMsg)
			}
		})
	}
}

func TestStatusIsSuccess(t *testing.T) {
	tests := []struct {
		name     string
		status   *Status
		expected bool
	}{
		{"nil status", nil, true},
		{"success status", NewStatus(Success), true},
		{"error status", NewStatus(Error, "error"), false},
		{"unschedulable status", NewStatus(Unschedulable, "unschedulable"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.status.IsSuccess()
			if result != tt.expected {
				t.Errorf("IsSuccess() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestStatusIsWait(t *testing.T) {
	tests := []struct {
		name     string
		status   *Status
		expected bool
	}{
		{"nil status", nil, false},
		{"wait status", NewStatus(Wait), true},
		{"success status", NewStatus(Success), false},
		{"error status", NewStatus(Error, "error"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.status.IsWait()
			if result != tt.expected {
				t.Errorf("IsWait() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestStatusIsSkip(t *testing.T) {
	tests := []struct {
		name     string
		status   *Status
		expected bool
	}{
		{"nil status", nil, false},
		{"skip status", NewStatus(Skip), true},
		{"success status", NewStatus(Success), false},
		{"error status", NewStatus(Error, "error"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.status.IsSkip()
			if result != tt.expected {
				t.Errorf("IsSkip() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestStatusIsRejected(t *testing.T) {
	tests := []struct {
		name     string
		status   *Status
		expected bool
	}{
		{"nil status", nil, false},
		{"unschedulable status", NewStatus(Unschedulable, "unschedulable"), true},
		{"unschedulable and unresolvable", NewStatus(UnschedulableAndUnresolvable, "unresolvable"), true},
		{"pending status", NewStatus(Pending, "pending"), true},
		{"success status", NewStatus(Success), false},
		{"error status", NewStatus(Error, "error"), false},
		{"wait status", NewStatus(Wait), false},
		{"skip status", NewStatus(Skip), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.status.IsRejected()
			if result != tt.expected {
				t.Errorf("IsRejected() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestStatusWithError(t *testing.T) {
	err := errors.New("test error")
	status := NewStatus(Error, "original reason")
	status.WithError(err)

	if status.err != err {
		t.Errorf("WithError() did not set error correctly")
	}

	// Test that error is included in reasons
	reasons := status.Reasons()
	if len(reasons) != 2 || reasons[0] != "test error" || reasons[1] != "original reason" {
		t.Errorf("Reasons() = %v, want [\"test error\", \"original reason\"]", reasons)
	}
}

func TestStatusAppendReason(t *testing.T) {
	status := NewStatus(Unschedulable, "first reason")
	status.AppendReason("second reason")

	reasons := status.Reasons()
	if len(reasons) != 2 || reasons[0] != "first reason" || reasons[1] != "second reason" {
		t.Errorf("Reasons() = %v, want [\"first reason\", \"second reason\"]", reasons)
	}
}

func TestStatusWithPlugin(t *testing.T) {
	status := NewStatus(Unschedulable, "test reason")
	status.WithPlugin("TestPlugin")

	if status.Plugin() != "TestPlugin" {
		t.Errorf("Plugin() = %v, want \"TestPlugin\"", status.Plugin())
	}
}

func TestStatusSetPlugin(t *testing.T) {
	status := NewStatus(Unschedulable, "test reason")
	status.SetPlugin("TestPlugin")

	if status.Plugin() != "TestPlugin" {
		t.Errorf("Plugin() = %v, want \"TestPlugin\"", status.Plugin())
	}
}

func TestAsStatus(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantNil  bool
		wantCode Code
	}{
		{"nil error", nil, true, Success},
		{"non-nil error", errors.New("test error"), false, Error},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := AsStatus(tt.err)

			if tt.wantNil {
				if status != nil {
					t.Errorf("AsStatus() = %v, want nil", status)
				}
				return
			}

			if status == nil {
				t.Error("AsStatus() returned nil for non-nil error")
				return
			}

			if status.Code() != tt.wantCode {
				t.Errorf("AsStatus().Code() = %v, want %v", status.Code(), tt.wantCode)
			}

			if status.err != tt.err {
				t.Errorf("AsStatus().err = %v, want %v", status.err, tt.err)
			}
		})
	}
}

func TestStatusAsError(t *testing.T) {
	tests := []struct {
		name     string
		status   *Status
		wantErr  bool
		wantMsg  string
	}{
		{"nil status", nil, false, ""},
		{"success status", NewStatus(Success), false, ""},
		{"wait status", NewStatus(Wait), false, ""},
		{"skip status", NewStatus(Skip), false, ""},
		{"error status", NewStatus(Error, "test error"), true, "test error"},
		{"unschedulable status", NewStatus(Unschedulable, "unschedulable"), true, "unschedulable"},
		{"status with error", NewStatus(Error).WithError(errors.New("internal error")), true, "internal error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.status.AsError()

			if tt.wantErr {
				if err == nil {
					t.Error("AsError() returned nil, want error")
					return
				}
				if err.Error() != tt.wantMsg {
					t.Errorf("AsError() = %v, want %v", err.Error(), tt.wantMsg)
				}
			} else {
				if err != nil {
					t.Errorf("AsError() = %v, want nil", err)
				}
			}
		})
	}
}

func TestStatusEqual(t *testing.T) {
	tests := []struct {
		name   string
		a      *Status
		b      *Status
		expect bool
	}{
		{
			name:   "both nil",
			a:      nil,
			b:      nil,
			expect: true,
		},
		{
			name:   "nil and success",
			a:      nil,
			b:      NewStatus(Success),
			expect: true,
		},
		{
			name:   "success and nil",
			a:      NewStatus(Success),
			b:      nil,
			expect: true,
		},
		{
			name:   "same status",
			a:      NewStatus(Unschedulable, "reason1", "reason2"),
			b:      NewStatus(Unschedulable, "reason1", "reason2"),
			expect: true,
		},
		{
			name:   "different codes",
			a:      NewStatus(Unschedulable, "reason"),
			b:      NewStatus(Error, "reason"),
			expect: false,
		},
		{
			name:   "different reasons",
			a:      NewStatus(Unschedulable, "reason1"),
			b:      NewStatus(Unschedulable, "reason2"),
			expect: false,
		},
		{
			name:   "different plugins",
			a:      NewStatus(Unschedulable, "reason").WithPlugin("PluginA"),
			b:      NewStatus(Unschedulable, "reason").WithPlugin("PluginB"),
			expect: false,
		},
		{
			name:   "with different error instances",
			a:      NewStatus(Error).WithError(errors.New("error1")),
			b:      NewStatus(Error).WithError(errors.New("error1")),
			expect: false, // Different error instances are not equal
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.a.Equal(tt.b)
			if result != tt.expect {
				t.Errorf("Equal() = %v, want %v", result, tt.expect)
			}

			// Test symmetry
			reverseResult := tt.b.Equal(tt.a)
			if reverseResult != result {
				t.Errorf("Equal() not symmetric: a.Equal(b) = %v, b.Equal(a) = %v", result, reverseResult)
			}
		})
	}
}

func TestStatusString(t *testing.T) {
	tests := []struct {
		name     string
		status   *Status
		expected string
	}{
		{"nil status", nil, ""},
		{"success status", NewStatus(Success), ""},
		{"single reason", NewStatus(Unschedulable, "Insufficient CPU"), "Insufficient CPU"},
		{"multiple reasons", NewStatus(Unschedulable, "Insufficient CPU", "Insufficient Memory"), "Insufficient CPU, Insufficient Memory"},
		{"with error", NewStatus(Error).WithError(errors.New("plugin failed")), "plugin failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.status.String()
			if result != tt.expected {
				t.Errorf("String() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestStatusReasons(t *testing.T) {
	tests := []struct {
		name     string
		status   *Status
		expected []string
	}{
		{
			name:     "status with error",
			status:   NewStatus(Error, "reason1", "reason2").WithError(errors.New("internal error")),
			expected: []string{"internal error", "reason1", "reason2"},
		},
		{
			name:     "status without error",
			status:   NewStatus(Unschedulable, "reason1", "reason2"),
			expected: []string{"reason1", "reason2"},
		},
		{
			name:     "nil status",
			status:   nil,
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reasons := tt.status.Reasons()
			if len(reasons) != len(tt.expected) {
				t.Errorf("Reasons() length = %v, want %v", len(reasons), len(tt.expected))
				return
			}
			for i, reason := range reasons {
				if reason != tt.expected[i] {
					t.Errorf("Reasons()[%d] = %v, want %v", i, reason, tt.expected[i])
				}
			}
		})
	}
}