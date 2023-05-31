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

    def is_all_unmarked(self):
        for i in range(self.__size // 8):
            if self.bits[i] != 0:
                return False
        for i in range(self.__size % 8):
            if (self.bits[self.__size // 8] & BitMap.BIT_UTIL[i]) != 0:
                return False
        return True
