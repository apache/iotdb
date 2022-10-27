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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.insertion;

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.Request.InsertionRequest;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemChunk;
import org.apache.iotdb.lsm.annotation.InsertionProcessor;
import org.apache.iotdb.lsm.context.requestcontext.InsertRequestContext;
import org.apache.iotdb.lsm.levelProcess.InsertLevelProcessor;

import java.util.List;

/** insertion for MemChunk */
@InsertionProcessor(level = 3)
public class MemChunkInsertion extends InsertLevelProcessor<MemChunk, Object, InsertionRequest> {

  /**
   * MemChunk is the last layer of memory nodes, no children
   *
   * @param memNode memory node
   * @param context request context
   * @return null
   */
  @Override
  public List<Object> getChildren(
      MemChunk memNode, InsertionRequest insertionRequest, InsertRequestContext context) {
    return null;
  }

  /**
   * the insert method corresponding to the MemChunk node
   *
   * @param memNode memory node
   * @param context insert request context
   */
  @Override
  public void insert(
      MemChunk memNode, InsertionRequest insertionRequest, InsertRequestContext context) {
    Integer deviceID = insertionRequest.getValue();
    memNode.put(deviceID);
  }
}
