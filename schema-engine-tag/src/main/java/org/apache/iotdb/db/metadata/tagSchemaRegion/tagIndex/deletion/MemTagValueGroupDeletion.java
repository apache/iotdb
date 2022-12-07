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
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemChunkGroup;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTagValueGroup;
import org.apache.iotdb.lsm.annotation.DeletionProcessor;
import org.apache.iotdb.lsm.context.requestcontext.DeleteRequestContext;
import org.apache.iotdb.lsm.levelProcess.DeleteLevelProcessor;

import java.util.ArrayList;
import java.util.List;

/** deletion for MemTagValueGroup */
@DeletionProcessor(level = 2)
public class MemTagValueGroupDeletion
    extends DeleteLevelProcessor<MemTagValueGroup, MemChunkGroup, DeletionRequest> {

  /**
   * get all MemChunkGroups that need to be processed in the current MemTagValueGroup
   *
   * @param memNode memory node
   * @param context request context
   * @return A list of saved MemChunks
   */
  @Override
  public List<MemChunkGroup> getChildren(
      MemTagValueGroup memNode, DeletionRequest deletionRequest, DeleteRequestContext context) {
    List<MemChunkGroup> memChunkGroups = new ArrayList<>();
    String tagValue = deletionRequest.getKey(context);
    MemChunkGroup child = memNode.get(tagValue);
    if (child != null) memChunkGroups.add(child);
    return memChunkGroups;
  }

  /**
   * the delete method corresponding to the MemTagValueGroup node
   *
   * @param memNode memory node
   * @param context deletion request context
   */
  @Override
  public void delete(
      MemTagValueGroup memNode, DeletionRequest deletionRequest, DeleteRequestContext context) {
    String tagValue = deletionRequest.getKey(context);
    MemChunkGroup child = memNode.get(tagValue);
    if (child == null || child.isEmpty()) {
      memNode.remove(tagValue);
    }
  }
}
