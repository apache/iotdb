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

import "strings"

// LtEqFilters compare the input value to the Reference value, and return true iff the input <= the Reference.
// Type mismatch will set the return value to false. Use lexicographical order for strings.
// Supported types: int32(int) int64(long) float32(float) float64(double) string.
type IntLtEqFilter struct {
	Ref int32
}

func (f *IntLtEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(int32); ok {
		return v <= f.Ref
	}
	return false
}

type LongLtEqFilter struct {
	Ref int64
}

func (f *LongLtEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(int64); ok {
		return v <= f.Ref
	}
	return false
}

type StrLtEqFilter struct {
	Ref string
}

func (f *StrLtEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(string); ok {
		return strings.Compare(v, f.Ref) <= 0
	}
	return false
}

type FloatLtEqFilter struct {
	Ref float32
}

func (f *FloatLtEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(float32); ok {
		return v <= f.Ref
	}
	return false
}

type DoubleLtEqFilter struct {
	Ref float64
}

func (f *DoubleLtEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(float64); ok {
		return v <= f.Ref
	}
	return false
}
