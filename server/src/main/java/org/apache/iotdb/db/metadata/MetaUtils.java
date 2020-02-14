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

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_WILDCARD;

class MetaUtils {

  private static final String PATH_SEPARATOR = "\\.";

  private MetaUtils() {

  }

  static String[] getNodeNames(String path) {
    String[] nodeNames;
    if (path.contains("\"") || path.contains("\'")) {
      path = path.trim().replace("\'", "\"");
      String measurement = path.split("\"")[1];
      String[] deviceNodeName = path.split("\"")[0].split(PATH_SEPARATOR);
      int nodeNumber = deviceNodeName.length + 1;
      nodeNames = new String[nodeNumber];
      System.arraycopy(deviceNodeName, 0, nodeNames, 0, nodeNumber - 1);
      nodeNames[nodeNumber - 1] = measurement;
    } else {
      nodeNames = path.split(PATH_SEPARATOR);
    }
    return nodeNames;
  }

  static String getNodeRegByIdx(int idx, String[] nodes) {
    return idx >= nodes.length ? PATH_WILDCARD : nodes[idx];
  }

}
