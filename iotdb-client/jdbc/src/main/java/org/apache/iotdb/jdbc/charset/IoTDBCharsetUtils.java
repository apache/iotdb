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

package org.apache.iotdb.jdbc.charset;

import java.nio.charset.Charset;
import java.util.List;

public class IoTDBCharsetUtils {

  public static String convertToUTF8(String str, String charset) {
    return IoTDBCharsetConstant.UTF_8.equalsIgnoreCase(charset)
        ? str
        : convertToCharset(str, IoTDBCharsetConstant.UTF_8);
  }

  public static String convertFromUTF8(String str, String charset) {
    return IoTDBCharsetConstant.UTF_8.equalsIgnoreCase(charset)
        ? str
        : convertToCharset(str, charset);
  }

  public static List<String> convertBatchFromUTF8(List<String> strList, String charset) {
    if (charset.equalsIgnoreCase(IoTDBCharsetConstant.UTF_8)) {
      return strList;
    }
    strList.replaceAll(str -> convertToCharset(str, charset));
    return strList;
  }

  public static String convertToCharset(String str, String charset) {
    if (str == null) {
      return null;
    }

    if (!Charset.isSupported(charset)) {
      throw new RuntimeException("Unsupported charset: " + charset);
    }

    try {
      return new String(str.getBytes(charset), charset);
    } catch (Exception e) {
      throw new RuntimeException("Failed to convert charset to " + charset, e);
    }
  }
}
