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

package bitpacking

import (
	_ "log"
)

/**
 *         This class is used to encode(decode) Integer in Java with specified bit-width.
 *         User need to guarantee that the length of every given Integer in binary mode
 *         is less than or equal to the bit-width.
 *         <p>
 *         e.g., if bit-width is 4, then Integer '16'(10000)b is not allowed but '15'(1111)b is allowed.
 *         <p>
 *         For a full example,
 *         Width: 3
 *         Input: 5 4 7 3 0 1 3 2
 *         <p>
 *         Output:
 *         <p>
 *         +-----------------------+   +-----------------------+   +-----------------------+
 *         |1 |0 |1 |1 |0 |0 |1 |1 |   |1 |0 |1 |1 |0 |0 |0 |0 |   |0 |1 |0 |1 |1 |0 |1 |0 |
 *         +-----------------------+   +-----------------------+   +-----------------------+
 *         +-----+  +-----+  +---------+  +-----+  +-----+  +---------+  +-----+  +-----+
 *         5        4          7          3        0          1          3        2
 *
 */

const (
	NUM_OF_INTS int = 8
)

type IntPacker struct {
	//bit-width
	BitWidth int
}

/**
 * Encode 8 ({@link IntPacker#NUM_OF_INTS}) Integers from the array 'values' with specified bit-width to bytes
 *
 * @param values - array where '8 Integers' are in
 * @param offset - the offset of first Integer to be encoded
 * @param buf    - encoded bytes, buf size must be equal to ({@link IntPacker#NUM_OF_INTS} * {@link IntPacker#width} / 8)
 */
func (p *IntPacker) Pack8Values(values []int32, offset int, buf []byte) {
	var bufIdx int = 0
	var valueIdx int = offset
	//remaining bits for the current unfinished Integer
	var leftBit int = 0

	for valueIdx < NUM_OF_INTS+offset {
		// buffer is used for saving 32 bits as a part of result
		var buffer int32 = 0
		// remaining size of bits in the 'buffer'
		var leftSize int = 32

		// encode the left bits of current Integer to 'buffer'
		if leftBit > 0 {
			buffer |= (values[valueIdx] << uint32(32-leftBit))
			leftSize -= leftBit
			leftBit = 0
			valueIdx++
		}

		for leftSize >= p.BitWidth && valueIdx < NUM_OF_INTS+offset {
			//encode one Integer to the 'buffer'
			buffer |= (values[valueIdx] << uint32(leftSize-p.BitWidth))
			leftSize -= p.BitWidth
			valueIdx++
		}
		// If the remaining space of the buffer can not save the bits for one Integer,
		if leftSize > 0 && valueIdx < NUM_OF_INTS+offset {
			// put the first 'leftSize' bits of the Integer into remaining space of the buffer
			buffer |= int32(uint32(values[valueIdx]) >> uint32(p.BitWidth-leftSize))
			leftBit = p.BitWidth - leftSize
			leftSize = 0
		}

		// put the buffer into the final result
		for j := 0; j < 4; j++ {
			buf[bufIdx] = (byte)((uint32(buffer) >> uint32((3-j)*8)) & 0xFF)
			bufIdx++
			if bufIdx >= p.BitWidth {
				return
			}
		}
	}
}

/**
 * decode Integers from byte array.
 *
 * @param buf    - array where bytes are in.
 * @param offset - offset of first byte to be decoded in buf
 * @param values - decoded result , the length of 'values' should be @{link IntPacker#NUM_OF_INTS}
 */
func (p *IntPacker) Unpack8Values(buf []byte, offset int, values []int32) {
	var byteIdx int = offset
	var buffer int64 = 0
	//total bits which have read from 'buf' to 'buffer'. i.e., number of available bits to be decoded.
	var totalBits int = 0
	var valueIdx int = 0

	for valueIdx < NUM_OF_INTS {
		//If current available bits are not enough to decode one Integer, then add next byte from buf to 'buffer'
		//until totalBits >= width
		for totalBits < p.BitWidth {
			buffer = ((buffer << 8) | int64(buf[byteIdx]&0xFF))
			byteIdx++
			totalBits += 8
		}

		//If current available bits are enough to decode one Integer, then decode one Integer one by one
		//until left bits in 'buffer' is not enough to decode one Integer.
		for totalBits >= p.BitWidth && valueIdx < 8 {
			values[valueIdx] = (int32)(uint32(buffer) >> uint(totalBits-p.BitWidth))
			valueIdx++
			totalBits -= p.BitWidth
			buffer = (buffer & ((1 << uint32(totalBits)) - 1))
		}
	}
}

/**
 * decode all values from 'buf' with specified offset and length
 * decoded result will be saved in the array named 'values'.
 *
 * @param buf:    array where all bytes are in.
 * @param offset: the offset of first byte to be decoded in buf.
 * @param length: length of bytes to be decoded in buf.
 * @param values: decoded result.
 */
func (p *IntPacker) UnpackAllValues(buf []byte, length int, values []int32) {
	var idx int = 0
	var k int = 0
	for idx < length {
		tv := make([]int32, 8)
		//decode 8 values one time, current result will be saved in the array named 'tv'
		p.Unpack8Values(buf, idx, tv)
		for i := 0; i < 8; i++ {
			values[k+i] = tv[i]
		}

		idx += p.BitWidth
		k += 8
	}
}
