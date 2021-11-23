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
package org.apache.iotdb.db.metadata.utils;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.utils.TestOnly;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class MetaUtils {

  private MetaUtils() {}

  /**
   * @param path the path will split. ex, root.ln.
   * @return string array. ex, [root, ln]
   * @throws IllegalPathException if path isn't correct, the exception will throw
   */
  public static String[] splitPathToDetachedPath(String path) throws IllegalPathException {
    List<String> nodes = new ArrayList<>();
    int startIndex = 0;
    for (int i = 0; i < path.length(); i++) {
      if (path.charAt(i) == IoTDBConstant.PATH_SEPARATOR) {
        String node = path.substring(startIndex, i);
        if (node.isEmpty()) {
          throw new IllegalPathException(path);
        }
        nodes.add(node);
        startIndex = i + 1;
        if (startIndex == path.length()) {
          throw new IllegalPathException(path);
        }
      }
    }
    if (startIndex <= path.length() - 1) {
      nodes.add(path.substring(startIndex));
    }
    return nodes.toArray(new String[0]);
  }

  /**
   * Get storage group path when creating schema automatically is enable
   *
   * <p>e.g., path = root.a.b.c and level = 1, return root.a
   *
   * @param path path
   * @param level level
   */
  public static PartialPath getStorageGroupPathByLevel(PartialPath path, int level)
      throws MetadataException {
    String[] nodeNames = path.getNodes();
    if (nodeNames.length <= level || !nodeNames[0].equals(IoTDBConstant.PATH_ROOT)) {
      throw new IllegalPathException(path.getFullPath());
    }
    String[] storageGroupNodes = new String[level + 1];
    System.arraycopy(nodeNames, 0, storageGroupNodes, 0, level + 1);
    return new PartialPath(storageGroupNodes);
  }

  /**
   * PartialPath of aligned time series will be organized to one AlignedPath. BEFORE this method,
   * all the aligned time series is NOT united. For example, given root.sg.d1.vector1[s1] and
   * root.sg.d1.vector1[s2], they will be organized to root.sg.d1.vector1 [s1,s2]
   *
   * @param fullPaths full path list without uniting the sub measurement under the same aligned time
   *     series. The list has been sorted by the alphabetical order, so all the aligned time series
   *     of one device has already been placed contiguously.
   * @return Size of partial path list could NOT equal to the input list size. For example, the
   *     vector1 (s1,s2) would be returned once.
   */
  public static List<PartialPath> groupAlignedPaths(List<PartialPath> fullPaths) {
    List<PartialPath> result = new LinkedList<>();
    AlignedPath alignedPath = null;
    for (PartialPath path : fullPaths) {
      MeasurementPath measurementPath = (MeasurementPath) path;
      if (!measurementPath.isUnderAlignedEntity()) {
        result.add(measurementPath);
        alignedPath = null;
      } else {
        if (alignedPath == null || !alignedPath.equals(measurementPath.getDevice())) {
          alignedPath = new AlignedPath(measurementPath);
          result.add(alignedPath);
        } else {
          alignedPath.addMeasurement(measurementPath);
        }
      }
    }
    return result;
  }

  @TestOnly
  public static List<String> getMultiFullPaths(IMNode node) {
    if (node == null) {
      return Collections.emptyList();
    }

    List<IMNode> lastNodeList = new ArrayList<>();
    collectLastNode(node, lastNodeList);

    List<String> result = new ArrayList<>();
    for (IMNode lastNode : lastNodeList) {
      result.add(lastNode.getFullPath());
    }

    return result;
  }

  @TestOnly
  public static void collectLastNode(IMNode node, List<IMNode> lastNodeList) {
    if (node != null) {
      Map<String, IMNode> children = node.getChildren();
      if (children.isEmpty()) {
        lastNodeList.add(node);
      }

      for (Entry<String, IMNode> entry : children.entrySet()) {
        IMNode childNode = entry.getValue();
        collectLastNode(childNode, lastNodeList);
      }
    }
  }
}
