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
	"io"
	_ "log"
	"math"
	"os"
	"tsfile/common/constant"
)

// file stream reader with buffer, supports random reading
const SIZE_BUF = 1024 * 8

type FileReader struct {
	reader *os.File
	pos    int64  // file position
	b      []byte // buffer
	l      int    // buffer len
	p      int    // buffer read position
}

func NewFileReader(reader *os.File) *FileReader {
	f := &FileReader{reader: reader}
	f.pos = 0
	f.l = 0
	f.p = 0
	f.b = make([]byte, SIZE_BUF)
	if n, err := f.reader.Read(f.b); err == nil || err == io.EOF {
		f.l = n
	} else {
		panic(err)
	}

	return f
}

func (f *FileReader) Close() error {
	return f.reader.Close()
}

func (f *FileReader) ReadSlice(length int) []byte {
	if length <= SIZE_BUF { // buffer size greater than reading size, so we get data from buffer
		// buffer remaining is not enough, we needs to read data from file into buffer first
		if f.l-f.p < length {
			if f.l > f.p {
				copy(f.b[0:], f.b[f.p:f.l])
			}
			f.l -= f.p
			f.p = 0

			if n, err := f.reader.Read(f.b[f.l:]); err == nil || err == io.EOF {
				f.l += n
				if f.l < length {
					panic("file has no enough data to read")
				}
			} else {
				panic(err)
			}
		}

		result := f.b[f.p : f.p+length]
		f.p += length
		f.pos += int64(length)

		return result
	} else { // buffer size less than reading size, so we need to read data from file, and discard buffer
		// get available data from buffer first
		result := make([]byte, length)
		if f.l > f.p {
			copy(result[0:], f.b[f.p:f.l])
		}

		remaining := f.l - f.p
		if n, err := f.reader.Read(result[remaining:]); err == nil || err == io.EOF {
			f.l = 0
			f.p = 0
			f.pos += int64(n + remaining)
		} else {
			panic(err)
		}

		return result
	}
}

func (f *FileReader) ReadBool() bool {
	buf := f.ReadSlice(constant.BOOLEAN_LEN)
	result := (buf[0] == 1)

	return result
}

func (f *FileReader) ReadShort() int16 {
	buf := f.ReadSlice(constant.SHORT_LEN)
	result := int16(binary.BigEndian.Uint16(buf))

	return result
}

func (f *FileReader) ReadInt() int32 {
	buf := f.ReadSlice(constant.INT_LEN)
	result := int32(binary.BigEndian.Uint32(buf)) //to int32, then to int('cause int==int64 on x64)

	return result
}

func (f *FileReader) ReadLong() int64 {
	buf := f.ReadSlice(constant.LONG_LEN)
	result := int64(binary.BigEndian.Uint64(buf))

	return result
}

func (f *FileReader) ReadFloat() float32 {
	buf := f.ReadSlice(constant.FLOAT_LEN)
	bits := binary.BigEndian.Uint32(buf)
	result := math.Float32frombits(bits)

	return result
}

func (f *FileReader) ReadDouble() float64 {
	buf := f.ReadSlice(constant.DOUBLE_LEN)
	bits := binary.BigEndian.Uint64(buf)
	result := math.Float64frombits(bits)

	return result
}

func (f *FileReader) ReadString() string {
	length := f.ReadInt()
	buf := f.ReadSlice(int(length))
	result := string(buf)

	return result
}

func (f *FileReader) ReadStringBinary() []byte {
	length := int(f.ReadInt())

	dst := make([]byte, length)
	buf := f.ReadSlice(length)

	copy(dst, buf)

	return dst
}

// this func does not change file pointer position and buffer
func (f *FileReader) ReadAt(length int, pos int64) []byte {
	buf := make([]byte, length)
	n, err := f.reader.ReadAt(buf, pos)
	if err != nil && err != io.EOF && n != length {
		panic(err)
	}
	//f.pos += int64(length)

	return buf
}

// buffer will be unavailable after seek
func (f *FileReader) Seek(pos int64, whence int) (ret int64, err error) {
	var e error
	f.pos, e = f.reader.Seek(pos, whence)
	f.l = 0
	f.p = 0

	return f.pos, e
}

func (f *FileReader) Pos() int64 {
	return f.pos
}

func (f *FileReader) Skip(length int32) (ret int64, err error) {
	return f.Seek(int64(length), io.SeekCurrent)
}
