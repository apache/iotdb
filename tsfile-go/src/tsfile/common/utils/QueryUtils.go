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

package utils

import (
	"sort"
	"strings"
)

// MergeStrings sorts and merges two string lists in ascent order.
func MergeStrings(strsA []string, strsB []string) []string {
	if strsB == nil {
		return strsA
	} else if strsA == nil {
		return strsB
	}

	lenA, lenB := len(strsA), len(strsB)
	indexA, indexB := 0, 0
	sort.Strings(strsA)
	sort.Strings(strsB)
	var ret []string
	for indexA < lenA && indexB < lenB {
		order := strings.Compare(strsA[indexA], strsB[indexB])
		if order == 0 {
			ret = append(ret, strsA[indexA])
			indexA++
			indexB++
		} else if order < 0 {
			ret = append(ret, strsA[indexA])
			indexA++
		} else {
			ret = append(ret, strsB[indexB])
			indexB++
		}
	}
	for indexA < lenA {
		ret = append(ret, strsA[indexA])
		indexA++
	}
	for indexB < lenB {
		ret = append(ret, strsB[indexB])
		indexB++
	}
	return ret
}

// TestCommonStrs tests if two string slices have common strings using sort and merge.
func TestCommonStrs(strsA []string, strsB []string) bool {
	if strsB == nil || strsA == nil {
		return false
	}
	lenA, lenB := len(strsA), len(strsB)
	indexA, indexB := 0, 0
	sort.Strings(strsA)
	sort.Strings(strsB)
	for indexA < lenA && indexB < lenB {
		order := strings.Compare(strsA[indexA], strsB[indexB])
		if order == 0 {
			return true
		} else if order < 0 {
			indexA++
		} else {
			indexB++
		}
	}
	return false
}
