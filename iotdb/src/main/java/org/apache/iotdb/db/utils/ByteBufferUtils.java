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
package org.apache.iotdb.db.utils;

import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.utils.BytesUtils;

public class ByteBufferUtils {

  /**
   * Put a string to ByteBuffer, first put a int which represents the bytes len of string, then put
   * the bytes of string to ByteBuffer
   *
   * @param buffer Target ByteBuffer to put a string value
   * @param value String value to be put
   */
  public static void putString(ByteBuffer buffer, String value) {
    if(value == null){
      buffer.putInt(-1);
    }else{
      byte[] vBytes = BytesUtils.stringToBytes(value);
      buffer.putInt(vBytes.length);
      buffer.put(vBytes);
    }
  }

  /**
   * Read a string value from ByteBuffer, first read a int that represents the bytes len of string,
   * then read bytes len value, finally transfer bytes to string
   *
   * @param buffer ByteBuffer to be read
   * @return string value
   */
  public static String readString(ByteBuffer buffer) {
    int valueLen = buffer.getInt();
    if(valueLen == -1)
      return null;
    byte[] valueBytes = new byte[valueLen];
    buffer.get(valueBytes);
    return BytesUtils.bytesToString(valueBytes);
  }

}
