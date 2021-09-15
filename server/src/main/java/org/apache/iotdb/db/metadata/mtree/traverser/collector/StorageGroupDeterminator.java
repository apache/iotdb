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
package org.apache.iotdb.db.metadata.mtree.traverser.collector;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_MULTI_LEVEL_WILDCARD;

public class StorageGroupDeterminator extends StorageGroupCollector<Map<String, String>> {

  public StorageGroupDeterminator(IMNode startNode, PartialPath path) throws MetadataException {
    super(startNode, path);
    this.resultSet = new HashMap<>();
    collectInternal = true;
  }

  @Override
  protected void processValidNode(IMNode node, int idx) throws MetadataException {
    // we have found one storage group, record it
    String sgName = node.getFullPath();
    // concat the remaining path with the storage group name
    StringBuilder pathWithKnownSG = new StringBuilder(sgName);
    for (int i = idx + 1; i < nodes.length; i++) {
      pathWithKnownSG.append(IoTDBConstant.PATH_SEPARATOR).append(nodes[i]);
    }
    if (idx >= nodes.length - 1 && nodes[nodes.length - 1].equals(PATH_MULTI_LEVEL_WILDCARD)) {
      // the we find the sg match the last node and the last node is a wildcard (find "root
      // .group1", for "root.**"), also append the wildcard (to make "root.group1.**")
      pathWithKnownSG.append(IoTDBConstant.PATH_SEPARATOR).append(PATH_MULTI_LEVEL_WILDCARD);
    }
    resultSet.put(sgName, pathWithKnownSG.toString());
  }
}
