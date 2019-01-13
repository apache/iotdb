/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.encoding.common;

/**
 * This class defines several constants using in encoding algorithm.
 *
 * @author xuyi
 */
public class EncodingConfig {
    // if number n repeats more than(>=) RLE_MAX_REPEATED_NUM times, use rle encoding, otherwise use bit-packing
    public static final int RLE_MAX_REPEATED_NUM = 8;

    // when to start a new bit-pacing group
    public static final int RLE_MAX_BIT_PACKED_NUM = 63;

    // bit width for Bitmap Encoding
    public static final int BITMAP_BITWIDTH = 1;
}
