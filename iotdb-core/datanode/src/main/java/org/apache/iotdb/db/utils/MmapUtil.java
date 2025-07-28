/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.utils;

import io.netty.util.internal.PlatformDependent;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

public class MmapUtil {

  public static void clean(MappedByteBuffer mappedByteBuffer) {
    if (mappedByteBuffer == null
        || !mappedByteBuffer.isDirect()
        || mappedByteBuffer.capacity() == 0) {
      return;
    }
    PlatformDependent.freeDirectBuffer(mappedByteBuffer);
  }

  /** we do not need to clean heapByteBuffer manually, so we just leave it alone. */
  public static void clean(ByteBuffer byteBuffer) {
    if (byteBuffer instanceof MappedByteBuffer) {
      clean((MappedByteBuffer) byteBuffer);
    }
  }
}
