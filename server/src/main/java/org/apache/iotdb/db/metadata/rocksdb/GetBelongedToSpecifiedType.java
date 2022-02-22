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
package org.apache.iotdb.db.metadata.rocksdb;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.utils.MetaUtils;

import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.iotdb.db.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.db.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

public class GetBelongedToSpecifiedType {

  private String fullPath;
  private String[] nodes;
  private RocksDBReadWriteHandler readWriteHandler;
  protected List<String> contextNodes = new ArrayList<>();
  private Set<PartialPath> allResult;
  private char nodeType;

  public GetBelongedToSpecifiedType(
      PartialPath partialPath, RocksDBReadWriteHandler readWriteHandler, char nodeType)
      throws RocksDBException, IllegalPathException {
    this.nodes = partialPath.getNodes();
    this.readWriteHandler = readWriteHandler;
    this.fullPath = partialPath.getFullPath();
    this.nodeType = nodeType;
    traverse();
  }

  private void traverse() throws RocksDBException, IllegalPathException {
    for (int idx = 0; idx <= nodes.length; idx++) {
      if (idx >= nodes.length - 1) {
        processNameMatch(idx);
        return;
      }
      String targetName = nodes[idx];
      if (MULTI_LEVEL_PATH_WILDCARD.equals(targetName)) {
        processMultiLevelWildcard(idx);
      } else if (targetName.contains(ONE_LEVEL_PATH_WILDCARD)) {
        processOneLevelWildcard(idx);
      } else {
        processNameMatch(idx);
      }
    }
  }

  public Set<PartialPath> getAllResult() {
    return allResult;
  }

  private void processNameMatch(int idx) throws RocksDBException, IllegalPathException {
    contextNodes.add(nodes[idx]);
    String innerName =
        RocksDBUtils.convertPartialPathToInnerByNodes(
            contextNodes.toArray(new String[0]), contextNodes.size(), nodeType);
    byte[] queryResult = readWriteHandler.get(null, innerName.getBytes());
    if (queryResult != null) {
      allResult.add(new PartialPath(new String(queryResult)));
    }
  }

  private void processOneLevelWildcard(int idx) throws IllegalPathException {
    // The current node name contains wildcards, all possible values queried from the previous node
    String innerName =
        RocksDBUtils.convertPartialPathToInnerByNodes(
            contextNodes.toArray(new String[0]), contextNodes.size(), nodeType);
    // prefixed match
    Set<String> matchedResult = readWriteHandler.getAllByPrefix(innerName);
    for (String str : matchedResult) {
      // split inner name to array
      String[] matchedKeyNodes = MetaUtils.splitPathToDetachedPath(str);
      // gets the current node name, and remove the first character -- level
      String matchedNodeName = matchedKeyNodes[idx].substring(1);
      // gets the current node name of the input path
      String patternNodeName = nodes[idx].replace("*", ".*");
      // if the match is successful, the path is valid
      if (Pattern.matches(patternNodeName, matchedNodeName)) {
        allResult.add(RocksDBUtils.getPartialPathFromInnerPath(str, matchedKeyNodes.length));
      }
    }
  }

  private void processMultiLevelWildcard(int idx) {
    // The current node name contains wildcards, all possible values queried from the previous node
    String innerName =
        RocksDBUtils.convertPartialPathToInnerByNodes(
            contextNodes.toArray(new String[0]), contextNodes.size(), nodeType);
    // prefixed match
    Set<String> matchedResult = readWriteHandler.getAllByPrefix(innerName);
    String lastNodeName = nodes[idx - 1];
  }
}
