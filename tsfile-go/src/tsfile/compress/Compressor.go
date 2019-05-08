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

package compress

import (
	"github.com/golang/snappy"
)

type Encompress struct{}

type Encompressor interface {
	GetEncompressedLength(srcLen int) int
	Encompress(dst []byte, src []byte) []byte
}

func (e *Encompress) DeCompress(compressed []byte) ([]byte, error) {
	return snappy.Decode(nil, compressed)
}

func (e *Encompress) GetEncompressor(tsCompressionType int16) Encompressor {
	var encompressor Encompressor
	switch tsCompressionType {
	case 0:
		encompressor = new(NoEncompressor)
	case 1:
		encompressor = new(SnappyEncompressor)
	//case 2:
	//	encompressor = new(NoEncompressor)
	//case 3:
	//	encompressor = new(SnappyEncompressor)
	//case 4:
	//	encompressor = new(NoEncompressor)
	//case 5:
	//	encompressor = new(SnappyEncompressor)
	default:
		encompressor = new(NoEncompressor)
		//log.Info("Encompressor not found, use default.")
	}
	return encompressor
}
