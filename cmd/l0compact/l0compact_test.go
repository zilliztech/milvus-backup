package l0compact

import "testing"

func TestValidateRejectsBadNames(t *testing.T) {
	// --output that would escape or nest outside a normal backup dir.
	for _, bad := range []string{"../victim", "a/b", "..", "/abs", ".hidden"} {
		o := &options{name: "src", output: bad}
		if err := o.validate(); err == nil {
			t.Errorf("validate() accepted invalid --output %q", bad)
		}
	}
	// --name is validated the same way.
	if err := (&options{name: "../x", output: "out"}).validate(); err == nil {
		t.Error("validate() accepted invalid --name ../x")
	}
	// A valid name/output pair passes.
	if err := (&options{name: "src", output: "src_l0c"}).validate(); err != nil {
		t.Errorf("validate() rejected valid names: %v", err)
	}
	// Default output is also validated (derived from a valid name).
	o := &options{name: "src"}
	if err := o.validate(); err != nil {
		t.Errorf("validate() rejected defaulted output: %v", err)
	}
	if o.output != "src_l0compacted" {
		t.Errorf("default output = %q", o.output)
	}
}
