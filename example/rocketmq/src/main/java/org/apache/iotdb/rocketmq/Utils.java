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
package org.apache.iotdb.rocketmq;

public class Utils {

  private Utils() {
    throw new IllegalStateException("Utility class");
  }

  public static int ConvertStringToInteger(String device) {
    int sum = 0;
    for (char c : device.toCharArray()) {
      sum += c;
    }
    return sum;
  }

  public static String getTimeSeries(String sql) {
    return sql.substring(0, sql.indexOf(',')).trim();
  }
}
