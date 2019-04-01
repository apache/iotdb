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

package filter

import (
	"tsfile/common/constant"
	"tsfile/timeseries/read/datatype"
)

// RowRecordValFilter receives a RowRecord and tests whether a certain column in the RowRecord specified by its name
// satisfies the inner filter.
// NOTICE：The schema of the input (number of columns and each's name) should remain the same for the same filter.
// E.g: If you use the filter (seriesName is "s2") on a RowRecord with three cols [s0, s1, s2], then you cannot use this
// filter on a RowRecord with cols [s1, s2, s3]. Because the filter remembers that "s2" is the third col and will not re-locate
// it in future tests.
type RowRecordValFilter struct {
	seriesName string
	filter     Filter

	seriesIndex int
}

func NewRowRecordValFilter(seriesName string, filter Filter) *RowRecordValFilter {
	return &RowRecordValFilter{seriesName: seriesName, filter: filter, seriesIndex: constant.INDEX_NOT_SET}
}

func (s *RowRecordValFilter) Satisfy(val interface{}) bool {
	if m, ok := val.(*datatype.RowRecord); ok {
		if s.seriesIndex == constant.INDEX_NOT_SET {
			for i, path := range m.Paths() {
				if path == s.seriesName {
					s.seriesIndex = i
					return s.filter.Satisfy(m.Values()[i])
				}
			}
		}

		return s.filter.Satisfy(m.Values()[s.seriesIndex])
	}
	return false
}
