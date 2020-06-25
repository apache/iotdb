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

import java.util.Arrays;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;

public class MetaUtils {

  public static final String PATH_SEPARATOR = "\\.";

  private MetaUtils() {
    throw new IllegalStateException("Utility class");
  }

  public static String[] getNodeNames(String path) {
    String[] nodeNames;
    int indexOfLeftSingleQuote = path.indexOf('\'');
    int indexOfRightSingleQuote = path.lastIndexOf('\'');
    int indexOfLeftDoubleQuote = path.indexOf('\"');
    int indexOfRightDoubleQuote = path.lastIndexOf('\"');
    String measurement;
    String device;
    String[] deviceNodeNames;
    if (indexOfRightSingleQuote != -1 && indexOfRightSingleQuote == path.length()-1) {
      measurement = path.substring(indexOfLeftSingleQuote);
      device = path.substring(0, indexOfLeftSingleQuote-1);
      deviceNodeNames = device.split(PATH_SEPARATOR);
      nodeNames = Arrays.copyOf(deviceNodeNames, deviceNodeNames.length + 1);
      nodeNames[nodeNames.length - 1] = measurement;
    } else if(indexOfRightDoubleQuote != -1 && indexOfRightDoubleQuote == path.length() -1) {
      measurement = path.substring(indexOfLeftDoubleQuote);
      device = path.substring(0, indexOfLeftDoubleQuote-1);
      deviceNodeNames = device.split(PATH_SEPARATOR);
      nodeNames = Arrays.copyOf(deviceNodeNames, deviceNodeNames.length + 1);
      nodeNames[nodeNames.length - 1] = measurement;
    } else {
      nodeNames = path.split(PATH_SEPARATOR);
    }
    return nodeNames;
  }

  static String getNodeRegByIdx(int idx, String[] nodes) {
    return idx >= nodes.length ? PATH_WILDCARD : nodes[idx];
  }

  /**
   * Get storage group name when creating schema automatically is enable
   *
   * e.g., path = root.a.b.c and level = 1, return root.a
   *
   * @param path path
   * @param level level
   */
  public static String getStorageGroupNameByLevel(String path, int level) throws MetadataException {
    String[] nodeNames = MetaUtils.getNodeNames(path);
    if (nodeNames.length <= level || !nodeNames[0].equals(IoTDBConstant.PATH_ROOT)) {
      throw new IllegalPathException(path);
    }
    StringBuilder storageGroupName = new StringBuilder(nodeNames[0]);
    for (int i = 1; i <= level; i++) {
      storageGroupName.append(IoTDBConstant.PATH_SEPARATOR).append(nodeNames[i]);
    }
    return storageGroupName.toString();
  }
}
