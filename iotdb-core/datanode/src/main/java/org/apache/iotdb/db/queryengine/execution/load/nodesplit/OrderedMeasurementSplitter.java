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

package org.apache.iotdb.db.queryengine.execution.load.nodesplit;

import org.apache.iotdb.db.queryengine.execution.load.ChunkData;
import org.apache.iotdb.db.queryengine.execution.load.TsFileData;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;

import java.util.ArrayList;
import java.util.List;

public class OrderedMeasurementSplitter implements PieceNodeSplitter {

  /**
   * Split a piece node by series.
   *
   * @return a list of piece nodes, each associated to only one series.
   */
  @Override
  public List<LoadTsFilePieceNode> split(LoadTsFilePieceNode pieceNode) {
    List<LoadTsFilePieceNode> result = new ArrayList<>();
    String currMeasurement = null;
    LoadTsFilePieceNode currNode =
        new LoadTsFilePieceNode(pieceNode.getPlanNodeId(), pieceNode.getTsFile());
    result.add(currNode);
    for (TsFileData tsFileData : pieceNode.getAllTsFileData()) {
      if (tsFileData.isModification()) {
        currNode.addTsFileData(tsFileData);
        continue;
      }

      ChunkData chunkData = (ChunkData) tsFileData;
      if (currMeasurement != null && !currMeasurement.equals(chunkData.firstMeasurement())) {
        currNode = new LoadTsFilePieceNode(pieceNode.getPlanNodeId(), pieceNode.getTsFile());
        result.add(currNode);
      }
      currMeasurement = chunkData.firstMeasurement();
      currNode.addTsFileData(tsFileData);
    }
    return result;
  }
}
