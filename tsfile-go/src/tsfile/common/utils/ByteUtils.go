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
	"bytes"
)

// get one bit in input byte. the offset is from low to high and start with 0
// e.g.<br>
// data:16(00010000), if offset is 4, return 1(000 "1" 0000) if offset is 7, return 0("0" 0010000)
func getByteN(data byte, offset int) int {
	offset &= 0x7

	if (data & (1 << uint32(7-offset))) != 0 {
		return 1
	} else {
		return 0
	}
}

/**
 * set one bit in input byte. the offset is from low to high and start with
 * index 0<br>
 * e.g.<br>
 * data:16(00010000), if offset is 4, value is 0, return 0({000 "0" 0000})
 * if offset is 1, value is 1, return 18({00010010}) if offset is 0, value
 * is 0, return 16(no change)
 *
 * @param data   input byte variable
 * @param offset bit offset
 * @param value  value to set
 * @return byte variable
 */
func setByteN(data byte, offset int, value int) byte {
	offset &= 0x7

	if value == 1 {
		return (byte)(data | (1 << uint32(7-offset)))
	} else {
		return (byte)(data & ^(1 << uint32(7-offset)))
	}
}

/**
 * get one bit in input integer. the offset is from low to high and start
 * with 0<br>
 * e.g.<br>
 * data:1000(00000000 00000000 00000011 11101000), if offset is 4, return
 * 0(111 "0" 1000) if offset is 9, return 1(00000 "1" 1 11101000)
 *
 * @param data   input int variable
 * @param offset bit offset
 * @return 0 or 1
 */
func getIntN(data int32, offset int) int32 {
	offset &= 0x1f

	if (data & (1 << uint32(offset))) != 0 {
		return 1
	} else {
		return 0
	}
}

// set one bit in input integer. the offset is from low to high and start with index 0
// e.g.<br>
// data:1000({00000000 00000000 00000011 11101000}),
// if offset is 4, value is 1, return 1016({00000000 00000000 00000011 111 "1" 1000})
// if offset is 9, value is 0 return 488({00000000 00000000 000000 "0" 1 11101000})
// if offset is 0, value is 0 return 1000(no change)
func setIntN(data int32, offset int, value int) int32 {
	offset &= 0x1f

	if value == 1 {
		return (data | (1 << uint32(offset)))
	} else {
		return (data & ^(1 << uint32(offset)))
	}
}

/**
 * get one bit in input long. the offset is from low to high and start with
 * 0<br>
 *
 * @param data   input long variable
 * @param offset bit offset
 * @return 0/1
 */
func getLongN(data int64, offset int) int32 {
	offset &= 0x3f

	if (data & (int64(1) << uint32(offset))) != 0 {
		return 1
	} else {
		return 0
	}
}

// set one bit in input long. the offset is from low to high and start with index 0
func setLongN(data int64, offset int, value int) int64 {
	offset &= 0x3f

	if value == 1 {
		return (data | (1 << uint32(offset)))
	} else {
		return (data & ^(1 << uint32(offset)))
	}
}

// given a byte array, read width bits from specified position bits and convert it to an integer
func BytesToInt(data []byte, pos int, width int) int32 {
	var value int32 = 0

	offset := pos + width - 1
	for i := 0; i < width; i++ {
		index := offset - i
		//value = setIntN(value, i, getByteN(data[index/8], index))

		if (data[index/8] & (1 << uint32(7-index&7))) != 0 {
			value = (value | (1 << uint32(i&0x1f)))
		} else {
			value = (value & ^(1 << uint32(i&0x1f)))
		}
	}

	return value
}

// given a byte array, read width bits from specified pos bits and convert it to an long
func BytesToLong(data []byte, pos int, width int) int64 {
	var value int64 = 0

	offset := pos + width - 1
	for i := 0; i < width; i++ {
		index := offset - i
		//value = setLongN(value, i, getByteN(data[index/8], index))

		if (data[index/8] & (1 << uint32(7-index&7))) != 0 {
			value = (value | (1 << uint32(i&0x3f)))
		} else {
			value = (value & ^(1 << uint32(i&0x3f)))
		}
	}

	return value
}

/**
 * convert an integer to a byte array which length is width, then copy this
 * array to the parameter result from pos
 *
 * @param srcNum input integer variable
 * @param result byte array to convert
 * @param pos    start position
 * @param width  bit-width
 */
func IntToBytes(srcNum int32, result []byte, pos int, width int) {
	offset := pos + width - 1

	for i := 0; i < width; i++ {
		temp := int32(offset-i) / 8
		result[temp] = setByteN(result[temp], offset-i, int(getIntN(srcNum, i)))
	}
}

/**
 * convert an long to a byte array which length is width, then copy this
 * array to the parameter result from pos
 *
 * @param srcNum input long variable
 * @param result byte array to convert
 * @param pos    start position
 * @param width  bit-width
 */
func LongToBytes(srcNum int64, result []byte, pos int, width int) {
	offset := pos + width - 1

	for i := 0; i < width; i++ {
		temp := (offset - i) / 8
		result[temp] = setByteN(result[temp], offset-i, int(getLongN(srcNum, i)))
	}
}

func NumberOfLeadingZeros(i int32) int32 {
	if i == 0 {
		return 32
	}

	var n int32 = 1
	if uint32(i)>>16 == 0 {
		n += 16
		i <<= 16
	}
	if uint32(i)>>24 == 0 {
		n += 8
		i <<= 8
	}
	if uint32(i)>>28 == 0 {
		n += 4
		i <<= 4
	}
	if uint32(i)>>30 == 0 {
		n += 2
		i <<= 2
	}
	n -= int32(uint32(i) >> 31)

	return n
}

func NumberOfTrailingZeros(i int32) int32 {
	if i == 0 {
		return 32
	}

	var y int32
	var n int32 = 31
	y = i << 16
	if y != 0 {
		n = n - 16
		i = y
	}
	y = i << 8
	if y != 0 {
		n = n - 8
		i = y
	}
	y = i << 4
	if y != 0 {
		n = n - 4
		i = y
	}
	y = i << 2
	if y != 0 {
		n = n - 2
		i = y
	}

	return n - int32(uint32(i<<1)>>31)
}

func NumberOfLeadingZerosLong(i int64) int32 {
	if i == 0 {
		return 64
	}

	var n int32 = 1
	var x int32 = int32(uint64(i) >> 32)

	if x == 0 {
		n += 32
		x = int32(i)
	}
	if uint32(x)>>16 == 0 {
		n += 16
		x <<= 16
	}
	if uint32(x)>>24 == 0 {
		n += 8
		x <<= 8
	}
	if uint32(x)>>28 == 0 {
		n += 4
		x <<= 4
	}
	if uint32(x)>>30 == 0 {
		n += 2
		x <<= 2
	}
	n -= int32(uint32(x) >> 31)

	return n
}

func NumberOfTrailingZerosLong(i int64) int32 {
	if i == 0 {
		return 64
	}

	var x, y int32
	var n int32 = 63
	y = int32(i)

	if y != 0 {
		n = n - 32
		x = y
	} else {
		x = (int32)(uint64(i) >> 32)
	}
	y = x << 16
	if y != 0 {
		n = n - 16
		x = y
	}
	y = x << 8
	if y != 0 {
		n = n - 8
		x = y
	}
	y = x << 4
	if y != 0 {
		n = n - 4
		x = y
	}
	y = x << 2
	if y != 0 {
		n = n - 2
		x = y
	}

	return n - int32(uint32(x<<1)>>31)
}

/**
* write a value to stream using unsigned var int format.
* for example,
* int 123456789 has its binary format 111010-1101111-0011010-0010101,
* function WriteUnsignedVarInt() will split every seven bits and write them to stream from low bit to high bit like:
* 1-0010101 1-0011010 1-1101111 0-0111010
* 1 represents has next byte to write, 0 represents number end.
 */
func WriteUnsignedVarInt(value int32, buffer *bytes.Buffer) {
	var position int32 = 1

	for (value & 0x7FFFFF80) != 0 {
		buffer.WriteByte(byte((value & 0x7F) | 0x80))
		value = int32(uint32(value) >> 7)
		position++
	}

	buffer.WriteByte(byte(value & 0x7F))
}

func WriteIntLittleEndianPaddedOnBitWidth(value int32, out *bytes.Buffer, bitWidth int) {
	paddedByteNum := (bitWidth + 7) / 8
	var offset uint8 = 0
	for {
		if paddedByteNum <= 0 {
			break
		}
		out.WriteByte(byte(value>>offset) & 0xFF)
		offset += 8
		paddedByteNum--
	}
}

func WriteLongLittleEndianPaddedOnBitWidth(value int64, out *bytes.Buffer, bitWidth int) {
	//paddedByteNum := (bitWidth + 7) / 8;
	out.Write(Int64ToByte(value, 0))
}
