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
package org.apache.iotdb.db.metadata.mtree.traverser;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.db.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;

/**
 * This class implements the storage group resolution function as following description.
 *
 * <p>For a path, infer all storage groups it may belong to. The path can have wildcards. Resolve
 * the path or path pattern into StorageGroupName-FullPath pairs that FullPath matches the given
 * path.
 *
 * <p>Consider the path into two parts: (1) the sub path which can not contain a storage group name
 * and (2) the sub path which is substring that begin after the storage group name.
 *
 * <p>(1) Suppose the part of the path can not contain a storage group name (e.g.,
 * "root".contains("root.sg") == false), then: For each one level wildcard *, only one level will be
 * inferred and the wildcard will be removed. For each multi level wildcard **, then the inference
 * will go on until the storage groups are found and the wildcard will be kept. (2) Suppose the part
 * of the path is a substring that begin after the storage group name. (e.g., For
 * "root.*.sg1.a.*.b.*" and "root.x.sg1" is a storage group, then this part is "a.*.b.*"). For this
 * part, keep what it is.
 *
 * <p>Assuming we have three SGs: root.group1, root.group2, root.area1.group3 Eg1: for input
 * "root.**", returns ("root.group1", "root.group1.**"), ("root.group2", "root.group2.**")
 * ("root.area1.group3", "root.area1.group3.**") Eg2: for input "root.*.s1", returns ("root.group1",
 * "root.group1.s1"), ("root.group2", "root.group2.s1")
 *
 * <p>Eg3: for input "root.area1.**", returns ("root.area1.group3", "root.area1.group3.**")
 *
 * <p>ResultSet: StorageGroupName-FullPath pairs
 */
public class PathGrouperByStorageGroup extends Traverser {

  private Map<String, String> resultSet = new HashMap<>();

  public PathGrouperByStorageGroup(IMNode startNode, PartialPath path) throws MetadataException {
    super(startNode, path);
  }

  @Override
  protected boolean processInternalMatchedMNode(IMNode node, int idx, int level) {
    if (node.isStorageGroup()) {
      transferToResult(node, idx);
      return true;
    }
    return false;
  }

  @Override
  protected boolean processFullMatchedMNode(IMNode node, int idx, int level) {
    if (node.isStorageGroup()) {
      transferToResult(node, idx);
      return true;
    }
    return false;
  }

  protected void transferToResult(IMNode node, int idx) {
    // we have found one storage group, record it
    String sgName = node.getFullPath();
    // concat the remaining path with the storage group name
    StringBuilder pathWithKnownSG = new StringBuilder(sgName);
    for (int i = idx + 1; i < nodes.length; i++) {
      pathWithKnownSG.append(IoTDBConstant.PATH_SEPARATOR).append(nodes[i]);
    }
    if (idx >= nodes.length - 1 && nodes[nodes.length - 1].equals(MULTI_LEVEL_PATH_WILDCARD)) {
      // the we find the sg match the last node and the last node is a wildcard (find "root
      // .group1", for "root.**"), also append the wildcard (to make "root.group1.**")
      pathWithKnownSG.append(IoTDBConstant.PATH_SEPARATOR).append(MULTI_LEVEL_PATH_WILDCARD);
    }
    resultSet.put(sgName, pathWithKnownSG.toString());
  }

  public Map<String, String> getResult() {
    return resultSet;
  }
}
