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
	"encoding/binary"
	_ "log"
	"math"
)

// bytes slice reader, result is the reference of source
type BytesReader struct {
	buf []byte
	pos int
}

func NewBytesReader(data []byte) *BytesReader {
	return &BytesReader{data, 0}
}

func (r *BytesReader) Pos() int {
	return r.pos
}

func (r *BytesReader) Len() int {
	return len(r.buf) - r.pos
}

func (r *BytesReader) Remaining() []byte {
	return r.buf[r.pos:]
}

func (r *BytesReader) ReadBool() bool {
	result := (r.buf[r.pos] == 1)
	r.pos += 1

	return result
}

func (r *BytesReader) ReadShort() int16 {
	result := int16(binary.BigEndian.Uint16(r.buf[r.pos : r.pos+2]))
	r.pos += 2

	return result
}

func (r *BytesReader) ReadInt() int32 {
	bytes := r.buf[r.pos : r.pos+4]
	result := int32(binary.BigEndian.Uint32(bytes))
	r.pos += 4

	return result
}

func (r *BytesReader) ReadLong() int64 {
	result := int64(binary.BigEndian.Uint64(r.buf[r.pos : r.pos+8]))
	r.pos += 8

	return result
}

func (r *BytesReader) ReadFloat() float32 {
	bits := binary.LittleEndian.Uint32(r.buf[r.pos : r.pos+4])
	result := math.Float32frombits(bits)
	r.pos += 4

	return result
}

func (r *BytesReader) ReadDouble() float64 {
	bits := binary.LittleEndian.Uint64(r.buf[r.pos : r.pos+8])
	result := math.Float64frombits(bits)
	r.pos += 8

	return result
}

func (r *BytesReader) ReadString() string {
	length := int(r.ReadInt())
	result := string(r.buf[r.pos : r.pos+length])
	r.pos += length

	return result
}

func (r *BytesReader) ReadBytes(length int) []byte {
	dst := make([]byte, length)
	copy(dst, r.buf[r.pos:r.pos+length])

	r.pos += length

	return dst
}

func (r *BytesReader) ReadStringBinary() []byte {
	length := int(r.ReadInt())

	dst := make([]byte, length)
	copy(dst, r.buf[r.pos:r.pos+length])

	r.pos += length

	return dst
}

func (r *BytesReader) ReadSlice(length int) []byte {
	result := r.buf[r.pos : r.pos+length]
	r.pos += length

	return result
}

// read a byte
func (r *BytesReader) Read() int32 {
	result := r.buf[r.pos]
	r.pos++

	return int32(result)
}

// for decoding
func (r *BytesReader) ReadUnsignedVarInt() int32 {
	var value int32 = 0
	var i uint32 = 0

	b := r.buf[r.pos]
	r.pos++

	for r.pos <= len(r.buf) && (b&0x80) != 0 {
		value |= int32(b&0x7F) << i
		i += 7

		b = r.buf[r.pos]
		r.pos++
	}

	return (value | int32(b)<<i)
}
