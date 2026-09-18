# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#


class BitMap(object):
    BIT_UTIL = [1, 1 << 1, 1 << 2, 1 << 3, 1 << 4, 1 << 5, 1 << 6, 1 << 7]

    def __init__(self, size):
        self.__size = size
        self.bits = []
        for i in range(size // 8 + 1):
            self.bits.append(0)

    def mark(self, position):
        self.bits[position // 8] |= BitMap.BIT_UTIL[position % 8]

    def is_marked(self, position):
        return (self.bits[position // 8] & BitMap.BIT_UTIL[position % 8]) != 0

    def get_size(self):
        return self.__size

    def is_all_unmarked(self):
        for i in range(self.__size // 8):
            if self.bits[i] != 0:
                return False
        for i in range(self.__size % 8):
            if (self.bits[self.__size // 8] & BitMap.BIT_UTIL[i]) != 0:
                return False
        return True

    def is_all_marked(self):
        return self.is_range_all_marked(0, self.__size)

    def is_range_all_marked(self, start, length):
        # Reject negatives: Python // on a negative start would index bits from
        # the tail (bits[-1]). Out-of-range matches C++ BitMap: return False.
        if (
            start < 0
            or length < 0
            or start > self.__size
            or length > self.__size - start
        ):
            return False
        if length == 0:
            return True

        end = start + length
        first_byte = start // 8
        last_byte = (end - 1) // 8
        if first_byte == last_byte:
            mask = ((1 << length) - 1) << (start & 7)
            return (self.bits[first_byte] & mask) == mask

        first_mask = (0xFF << (start & 7)) & 0xFF
        if (self.bits[first_byte] & first_mask) != first_mask:
            return False
        for index in range(first_byte + 1, last_byte):
            if self.bits[index] != 0xFF:
                return False
        last_bit_count = end & 7
        last_mask = 0xFF if last_bit_count == 0 else (1 << last_bit_count) - 1
        return (self.bits[last_byte] & last_mask) == last_mask
