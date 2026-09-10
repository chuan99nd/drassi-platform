package planner

import (
	"sync"

	"github.com/nektos/act/pkg/model"
	"gopkg.in/yaml.v3"
)

var installHookOnce sync.Once

func init() {
	installDecodeErrorHook()
}

// installDecodeErrorHook overrides act's model.OnDecodeNodeError package
// global exactly once. Upstream's default (pkg/model/workflow.go ~:750)
// calls log.Fatalf, which would kill the entire drassi-server process the
// moment any job field (needs/runs-on/on/...) fails to decode into its
// target type via the internal decodeNode() helper.
//
// Our replacement is a deliberate no-op: every decodeNode() caller
// (Job.Needs, Job.RunsOn, Workflow.On, Workflow.OnEvent, ...) already treats
// a false return as "use the zero value" and keeps going, and a workflow
// that is actually structurally broken is caught earlier by pkg/schema
// validation inside model.ReadWorkflow (invoked from Workflow.UnmarshalYAML),
// which returns a normal error that Plan wraps in ErrInvalidWorkflow. So
// nothing downstream depends on this hook doing anything beyond not calling
// log.Fatalf/os.Exit.
//
// Guarded by sync.Once because model.OnDecodeNodeError is a package-level
// var shared by every caller of pkg/model in this process; install it once,
// not per Planner instance.
func installDecodeErrorHook() {
	installHookOnce.Do(func() {
		model.OnDecodeNodeError = func(_ yaml.Node, _ interface{}, _ error) {
			// Intentionally does nothing -- see doc comment above.
		}
	})
}
