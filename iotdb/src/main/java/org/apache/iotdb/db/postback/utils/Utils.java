/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.postback.utils;

/**
 * Created by stefanie on 07/08/2017.
 */

public class Utils {

  public static String getType(String properties) {
    return properties.split(",")[0];
  }

  public static String getEncode(String properties) {
    return properties.split(",")[1];
  }

  public static String getPath(String timeseries) {
    int lastPointIndex = timeseries.lastIndexOf(".");
    return timeseries.substring(0, lastPointIndex);
  }

  public static String getSensor(String timeseries) {
    int lastPointIndex = timeseries.lastIndexOf(".");
    return timeseries.substring(lastPointIndex + 1);
  }

  /**
   * main function.
   *
   * @param argc -console argc
   */
  public static void main(String[] argc) {

    String test2 = "root.excavator.Beijing.d1.s1";
    System.out.println(getPath(test2) + " " + getSensor(test2));

  }
}
