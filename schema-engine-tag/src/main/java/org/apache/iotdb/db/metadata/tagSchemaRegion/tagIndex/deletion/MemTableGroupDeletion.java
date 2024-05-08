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
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTable;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTableGroup;
import org.apache.iotdb.lsm.annotation.DeletionProcessor;
import org.apache.iotdb.lsm.context.requestcontext.DeleteRequestContext;
import org.apache.iotdb.lsm.levelProcess.DeleteLevelProcessor;

import java.util.ArrayList;
import java.util.List;

/** deletion for MemTableGroup */
@DeletionProcessor(level = 0)
public class MemTableGroupDeletion
    extends DeleteLevelProcessor<MemTableGroup, MemTable, DeletionRequest> {

  /**
   * get all MemTables that need to be processed in the current MemTableGroup
   *
   * @param memNode memory node
   * @param context request context
   * @return A list of saved MemTables
   */
  @Override
  public List<MemTable> getChildren(
      MemTableGroup memNode, DeletionRequest request, DeleteRequestContext context) {
    List<MemTable> memTables = new ArrayList<>();
    int id = request.getValue();
    if (memNode.inWorkingMemTable(id)) {
      memTables.add(memNode.getWorkingMemTable());
    } else {
      memTables.add(
          memNode.getImmutableMemTables().get(id / memNode.getNumOfDeviceIdsInMemTable()));
    }
    return memTables;
  }

  /**
   * the delete method corresponding to the MemTableGroup node, do nothing
   *
   * @param memNode memory node
   * @param context deletion request context
   */
  @Override
  public void delete(
      MemTableGroup memNode, DeletionRequest request, DeleteRequestContext context) {}
}
