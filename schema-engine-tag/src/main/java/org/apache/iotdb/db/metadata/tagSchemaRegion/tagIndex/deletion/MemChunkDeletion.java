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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.deletion;

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.Request.DeletionRequest;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemChunk;
import org.apache.iotdb.lsm.annotation.DeletionProcessor;
import org.apache.iotdb.lsm.context.requestcontext.DeleteRequestContext;
import org.apache.iotdb.lsm.levelProcess.DeleteLevelProcessor;

import java.util.List;

/** deletion for MemChunk */
@DeletionProcessor(level = 3)
public class MemChunkDeletion extends DeleteLevelProcessor<MemChunk, Object, DeletionRequest> {

  /**
   * MemChunk is the last layer of memory nodes, no children
   *
   * @param memNode memory node
   * @param context request context
   * @return null
   */
  @Override
  public List<Object> getChildren(
      MemChunk memNode, DeletionRequest request, DeleteRequestContext context) {
    return null;
  }

  /**
   * the delete method corresponding to the MemChunk node
   *
   * @param memNode memory node
   * @param context deletion request context
   */
  @Override
  public void delete(MemChunk memNode, DeletionRequest request, DeleteRequestContext context) {
    Integer deviceID = request.getValue();
    memNode.remove(deviceID);
  }
}
