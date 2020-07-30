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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

public class MetaUtils {

  public static final String PATH_SEPARATOR = "\\.";

  private MetaUtils() {
    throw new IllegalStateException("Utility class");
  }

  static String getNodeRegByIdx(int idx, String[] nodes) {
    return idx >= nodes.length ? PATH_WILDCARD : nodes[idx];
  }

  @TestOnly
  public static List<String> splitPathByDot(String path) {
    List<String> deviceNodes = new ArrayList<>();
    Collections.addAll(deviceNodes, path.split(PATH_SEPARATOR));
    return deviceNodes;
  }

  public static List<String> splitPathToNodes(String path) throws IllegalPathException {
    List<String> nodes = new ArrayList<>();
    int startIndex = 0;
    for (int i = 0; i < path.length(); i++) {
      if (path.charAt(i) == IoTDBConstant.PATH_SEPARATOR) {
        nodes.add(path.substring(startIndex, i));
        startIndex = i + 1;
      } else if (path.charAt(i) == '"') {
        int endIndex = path.indexOf('"', i + 1);
        if (endIndex != -1 && (endIndex == path.length() - 1 || path.charAt(endIndex + 1) == '.')) {
          nodes.add(path.substring(startIndex, endIndex + 1));
          i = endIndex + 1;
          startIndex = endIndex + 2;
        } else {
          throw new IllegalPathException("Illegal path: " + path);
        }
      } else if (path.charAt(i) == '\'') {
        throw new IllegalPathException("Illegal path with single quote: " + path);
      }
    }
    if (startIndex <= path.length() - 1) {
      nodes.add(path.substring(startIndex));
    }
    return nodes;
  }

  /**
   * Get storage group name when creating schema automatically is enable
   *
   * e.g., nodes = [root, a, b, c] and level = 1, return [root, a]
   *
   * @param nodeNames nodeNames
   * @param level level
   */
  public static List<String> getStorageGroupNameNodesByLevel(List<String> nodeNames, int level) throws MetadataException {
    if (nodeNames.size() <= level || !nodeNames.get(0).equals(IoTDBConstant.PATH_ROOT)) {
      throw new IllegalPathException(concatNodesByDot(nodeNames));
    }
    return new ArrayList<>(nodeNames.subList(0, level+1));
  }

  /**
   * Combine a path by a string list
   * e.g., nodes = [root, a, b, c], return root.a.b.c
   */

  public static String concatNodesByDot(List<String> nodes) {
    return String.join(TsFileConstant.PATH_SEPARATOR, nodes);
  }

}
