// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bindinfo

import (
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

var (
	lastPlanBindingUpdateTime = "last_plan_binding_update_time"
)

// GetScope gets the status variables scope.
func (*BindHandle) GetScope(_ string) variable.ScopeFlag {
	return variable.ScopeSession
}

// Stats returns the server statistics.
func (h *BindHandle) Stats(_ *variable.SessionVars) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	m[lastPlanBindingUpdateTime] = h.getLastUpdateTime().String()
	return m, nil
}
