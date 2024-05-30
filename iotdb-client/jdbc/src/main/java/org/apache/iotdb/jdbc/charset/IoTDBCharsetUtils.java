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
    return charset.equalsIgnoreCase(IoTDBCharsetConstant.UTF_8)
        ? str
        : convertCharset(str, charset, IoTDBCharsetConstant.UTF_8);
  }

  public static String convertFromUTF8(String str, String charset) {
    return charset.equalsIgnoreCase(IoTDBCharsetConstant.UTF_8)
        ? str
        : convertCharset(str, IoTDBCharsetConstant.UTF_8, charset);
  }

  public static List<String> convertBatchFromUTF8(List<String> strList, String charset) {
    if (charset.equalsIgnoreCase(IoTDBCharsetConstant.UTF_8)) {
      return strList;
    }
    strList.replaceAll(str -> convertCharset(str, IoTDBCharsetConstant.UTF_8, charset));
    return strList;
  }

  public static String convertCharset(String str, String fromCharset, String toCharset) {
    if (str == null) {
      return null;
    }

    if (!Charset.isSupported(fromCharset) || !Charset.isSupported(toCharset)) {
      throw new RuntimeException("Unsupported charset: " + fromCharset + " or " + toCharset);
    }

    try {
      return new String(str.getBytes(fromCharset), toCharset);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to convert charset from " + fromCharset + " to " + toCharset, e);
    }
  }
}
