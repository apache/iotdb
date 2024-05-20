/*
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

package org.apache.iotdb.db.queryengine.transformation.datastructure.util;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MB;

public class BinaryUtils {
  public static final int MIN_OBJECT_HEADER_SIZE = 8;

  public static final int MIN_ARRAY_HEADER_SIZE = MIN_OBJECT_HEADER_SIZE + 4;

  public static int calculateCapacity(float memoryLimitInMB, int byteArrayLength) {
    float memoryLimitInB = memoryLimitInMB * MB / 2;
    return TSFileConfig.ARRAY_CAPACITY_THRESHOLD
        * (int)
            (memoryLimitInB
                / (TSFileConfig.ARRAY_CAPACITY_THRESHOLD
                    * calculateSingleBinaryTVPairMemory(byteArrayLength)));
  }

  public static float calculateSingleBinaryTVPairMemory(int byteArrayLength) {
    return ReadWriteIOUtils.LONG_LEN // time
        + MIN_OBJECT_HEADER_SIZE // value: header length of Binary
        + MIN_ARRAY_HEADER_SIZE // value: header length of values in Binary
        + byteArrayLength // value: length of values array in Binary
        + ReadWriteIOUtils.BIT_LEN; // extra bit(1/8 Byte): whether the Binary is null
  }
}
