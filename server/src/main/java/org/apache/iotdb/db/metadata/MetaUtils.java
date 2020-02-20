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
package org.apache.iotdb.db.metadata;

public class MetaUtils {
  public static String[] getNodeNames(String path, String separator) {
    String[] nodeNames;
    path = path.trim();
    if (path.contains("\"") || path.contains("\'")) {
      String[] deviceAndMeasurement;
      if (path.contains("\"")) {
        deviceAndMeasurement = path.split("\"");
      } else {
        deviceAndMeasurement = path.split("\'");
      }
      String device = deviceAndMeasurement[0];
      String measurement = deviceAndMeasurement[1];
      String[] deviceNodeName = device.split(separator);
      int nodeNumber = deviceNodeName.length + 1;
      nodeNames = new String[nodeNumber];
      System.arraycopy(deviceNodeName, 0, nodeNames, 0, nodeNumber - 1);
      nodeNames[nodeNumber - 1] = measurement;
    } else {
      nodeNames = path.split(separator);
    }
    return nodeNames;
  }
}
