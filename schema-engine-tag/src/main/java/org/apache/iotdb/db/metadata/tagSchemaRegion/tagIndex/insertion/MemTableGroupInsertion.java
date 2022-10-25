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
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTable;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTableGroup;
import org.apache.iotdb.lsm.annotation.InsertionProcessor;
import org.apache.iotdb.lsm.context.requestcontext.InsertRequestContext;
import org.apache.iotdb.lsm.levelProcess.InsertLevelProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** insertion for MemTableGroup */
@InsertionProcessor(level = 0)
public class MemTableGroupInsertion
    extends InsertLevelProcessor<MemTableGroup, MemTable, InsertionRequest> {

  /**
   * get all MemTable that need to be processed in the current MemTableGroup
   *
   * @param memNode memory node
   * @param context request context
   * @return A list of saved MemTables
   */
  @Override
  public List<MemTable> getChildren(
      MemTableGroup memNode, InsertionRequest request, InsertRequestContext context) {
    List<MemTable> memTables = new ArrayList<>();
    memTables.add(memNode.getWorkingMemTable());
    return memTables;
  }

  /**
   * the insert method corresponding to the MemTableGroup node
   *
   * @param memNode memory node
   * @param context insert request context
   */
  @Override
  public void insert(
      MemTableGroup memNode, InsertionRequest request, InsertRequestContext context) {
    int id = request.getValue();
    MemTable workingMemTable = memNode.getWorkingMemTable();
    Map<Integer, MemTable> immutableMemTables = memNode.getImmutableMemTables();
    // if the device id can not be saved to the current working MemTable
    if (!memNode.inWorkingMemTable(id)) {
      workingMemTable.setStatus(MemTable.IMMUTABLE);
      immutableMemTables.put(
          memNode.getMaxDeviceID() / memNode.getNumOfDeviceIdsInMemTable(), workingMemTable);
      memNode.setWorkingMemTable(new MemTable(MemTable.WORKING));
    }
    memNode.setMaxDeviceID(id);
  }
}
