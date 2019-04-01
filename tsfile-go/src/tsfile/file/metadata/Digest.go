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

package metadata

import (
	"bytes"
	_ "log"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type TsDigest struct {
	//statistics     map[string][]byte
	statistics     map[string]*bytes.Buffer
	serializedSize int
	sizeOfList     int
}

func (f *TsDigest) Deserialize(reader *utils.BytesReader) {
	f.serializedSize = constant.INT_LEN

	f.statistics = make(map[string]*bytes.Buffer)
	if size := int(reader.ReadInt()); size > 0 {
		for i := 0; i < size; i++ {
			key := reader.ReadString()
			value := reader.ReadStringBinary()

			f.statistics[key] = bytes.NewBuffer(value)
			f.serializedSize += constant.INT_LEN + len(key) + constant.INT_LEN + len(value)
		}
	}
}

func (t *TsDigest) SetStatistics(statistics map[string]*bytes.Buffer) {
	t.statistics = statistics
	// recalculate serialized size
	t.ReCalculateSerializedSize()
}

func (t *TsDigest) ReCalculateSerializedSize() {
	//calculate size again
	t.serializedSize = 4
	if t.statistics != nil {
		for k, v := range t.statistics {
			//log.Info("key: %s, k: %d, v: %d", k, len(k), v.Len())
			t.serializedSize += 4 + len(k) + 4 + v.Len()
		}
		t.sizeOfList = len(t.statistics)
	} else {
		t.sizeOfList = 0
	}
}

func (t *TsDigest) GetNullDigestSize() int {
	return 4
}

func (t *TsDigest) serializeTo(buf *bytes.Buffer) int {
	if (t.statistics != nil && t.sizeOfList != len(t.statistics)) || (t.statistics == nil && t.sizeOfList != 0) {
		t.ReCalculateSerializedSize()
	}

	var byteLen int
	if t.statistics == nil || len(t.statistics) == 0 {
		n1, _ := buf.Write(utils.Int32ToByte(0, 0))
		byteLen += n1
	} else {
		n2, _ := buf.Write(utils.Int32ToByte(int32(len(t.statistics)), 0))
		byteLen += n2
		for k, v := range t.statistics {
			n3, _ := buf.Write(utils.Int32ToByte(int32(len(k)), 0))
			byteLen += n3
			n4, _ := buf.Write([]byte(k))
			byteLen += n4

			n5, _ := buf.Write(utils.Int32ToByte(int32(v.Len()), 0))
			byteLen += n5

			timeSlice := make([]byte, v.Len())
			v.Read(timeSlice)
			n6, _ := buf.Write(timeSlice)
			byteLen += n6
			// delete(t.statistics, k)
		}
	}

	return byteLen
}

func (t *TsDigest) GetSerializedSize() int {
	if t.statistics == nil || t.sizeOfList != len(t.statistics) {
		t.ReCalculateSerializedSize()
	}
	return t.serializedSize
}

func NewTsDigest() (*TsDigest, error) {
	return &TsDigest{
		statistics:     make(map[string]*bytes.Buffer),
		sizeOfList:     0,
		serializedSize: 4,
	}, nil
}
