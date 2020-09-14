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
package org.apache.iotdb.db.http.utils;

import org.apache.iotdb.db.http.constant.HttpConstant;

public class URIUtils {
  private URIUtils() {
    throw new IllegalStateException("Utility class");
  }
  public static String removeParameter(String uri) {
    int index = uri.indexOf(HttpConstant.QUESTION_MARK);
    if(index < 0) {
      if(uri.charAt(uri.length() - 1) == '/') {
        return uri.substring(0, uri.length() - 1);
      }
    } else {
      return uri.substring(0, index);
    }
    return uri;
  }
}
