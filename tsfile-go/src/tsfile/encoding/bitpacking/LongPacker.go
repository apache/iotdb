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
 * This class is used to encode(decode) Long in Java with specified bit-width.
 * User need to guarantee that the length of every given Long in binary mode
 * is less than or equal to the bit-width.
 * <p>
 * e.g., if bit-width is 31, then Long '2147483648'(2^31) is not allowed but '2147483647'(2^31-1) is allowed.
 * <p>
 * For a full example,
 * Width: 3
 * Input: 5 4 7 3 0 1 3 2
 * <p>
 * Output:
 * <p>
 * +-----------------------+   +-----------------------+   +-----------------------+
 * |1 |0 |1 |1 |0 |0 |1 |1 |   |1 |0 |1 |1 |0 |0 |0 |0 |   |0 |1 |0 |1 |1 |0 |1 |0 |
 * +-----------------------+   +-----------------------+   +-----------------------+
 * +-----+  +-----+  +---------+  +-----+  +-----+  +---------+  +-----+  +-----+
 * 5        4          7          3        0          1          3        2
 *
 */

const (
	NUM_OF_LONGS int = 8
)

type LongPacker struct {
	//bit-width
	BitWidth int
}

/**
 * Encode 8 ({@link LongPacker#NUM_OF_LONGS}) Integers from the array 'values' with specified bit-width to bytes
 *
 * @param values - array where '8 Integers' are in
 * @param offset - the offset of first Integer to be encoded
 * @param buf    - encoded bytes, buf size must be equal to ({@link LongPacker#NUM_OF_LONGS} * {@link LongPacker#width} / 8)
 */
func (p *LongPacker) Pack8Values(values []int64, offset int, buf []byte) {
	var bufIdx int = 0
	var valueIdx int = offset
	//remaining bits for the current unfinished Integer
	var leftBit int = 0

	for valueIdx < NUM_OF_LONGS+offset {
		// buffer is used for saving 32 bits as a part of result
		var buffer int64 = 0
		// remaining size of bits in the 'buffer'
		var leftSize int = 64

		// encode the left bits of current Integer to 'buffer'
		if leftBit > 0 {
			buffer |= (values[valueIdx] << uint(64-leftBit))
			leftSize -= leftBit
			leftBit = 0
			valueIdx++
		}

		for leftSize >= p.BitWidth && valueIdx < NUM_OF_LONGS+offset {
			//encode one Long to the 'buffer'
			buffer |= (values[valueIdx] << uint(leftSize-p.BitWidth))
			leftSize -= p.BitWidth
			valueIdx++
		}
		// If the remaining space of the buffer can not save the bits for one Integer,
		if leftSize > 0 && valueIdx < NUM_OF_LONGS+offset {
			// put the first 'leftSize' bits of the Long into remaining space of the buffer
			buffer |= int64(uint64(values[valueIdx]) >> uint(p.BitWidth-leftSize))
			leftBit = p.BitWidth - leftSize
			leftSize = 0
		}

		// put the buffer into the final result
		for j := 0; j < 8; j++ {
			buf[bufIdx] = (byte)((uint64(buffer) >> uint((8-j-1)*8)) & 0xFF)
			bufIdx++
			if bufIdx >= p.BitWidth*8/8 {
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
 * @param values - decoded result , the length of 'values' should be @{link LongPacker#NUM_OF_LONGS}
 */
func (p *LongPacker) Unpack8Values(buf []byte, offset int, values []int64) {
	var byteIdx int = offset
	var valueIdx int = 0
	//left bit(s) available for current byte in 'buf'
	var leftBits int = 8
	//bits that has been read for current long value which is to be decoded
	var totalBits int = 0

	//decode long value one by one
	for valueIdx < 8 {
		//set all the 64 bits in current value to '0'
		values[valueIdx] = 0
		//read until 'totalBits' is equal to width
		for totalBits < p.BitWidth {
			//If 'leftBits' in current byte belongs to current long value
			if p.BitWidth-totalBits >= leftBits {
				//then put left bits in current byte to current long value
				values[valueIdx] = values[valueIdx] << uint32(leftBits)
				values[valueIdx] = (values[valueIdx] | ((1<<uint32(leftBits) - 1) & int64(buf[byteIdx])))
				totalBits += leftBits
				//get next byte
				byteIdx++
				//set 'leftBits' in next byte to 8 because the next byte has not been used
				leftBits = 8
				//Else take part of bits in 'leftBits' to current value.
			} else {
				//numbers of bits to be take
				t := p.BitWidth - totalBits
				values[valueIdx] = values[valueIdx] << uint32(t)

				temp := int64((1<<uint(leftBits) - 1) & buf[byteIdx])
				temp = temp >> uint(leftBits-t)
				values[valueIdx] = values[valueIdx] | temp

				leftBits -= t
				totalBits += t
			}
		}
		//Start to decode next long value
		valueIdx++
		totalBits = 0
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
func (p *LongPacker) UnpackAllValues(buf []byte, length int, values []int64) {
	var idx int = 0
	var k int = 0
	for idx < length {
		tv := make([]int64, 8)
		//decode 8 values one time, current result will be saved in the array named 'tv'
		p.Unpack8Values(buf, idx, tv)

		for i := 0; i < 8; i++ {
			values[k+i] = tv[i]
		}
		idx += p.BitWidth
		k += 8
	}
}
