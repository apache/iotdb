/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package operator

import "tsfile/timeseries/filter"

// OrFilter returns true iff the value satisfies at least one of its children or it has no children.
type OrFilter struct {
	filters []filter.Filter
}

func (f *OrFilter) Satisfy(val interface{}) bool {
	if f.filters == nil {
		return true
	}

	for _, filt := range f.filters {
		if filt.Satisfy(val) {
			return true
		}
	}
	return false
}
