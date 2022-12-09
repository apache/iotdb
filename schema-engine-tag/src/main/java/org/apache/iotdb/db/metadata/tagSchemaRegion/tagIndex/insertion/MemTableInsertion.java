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
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTagValueGroup;
import org.apache.iotdb.lsm.annotation.InsertionProcessor;
import org.apache.iotdb.lsm.context.requestcontext.InsertRequestContext;
import org.apache.iotdb.lsm.levelProcess.InsertLevelProcessor;

import java.util.ArrayList;
import java.util.List;

/** insertion for MemTable */
@InsertionProcessor(level = 1)
public class MemTableInsertion
    extends InsertLevelProcessor<MemTable, MemTagValueGroup, InsertionRequest> {

  /**
   * get all MemTagValueGroups that need to be processed in the current MemTable
   *
   * @param memNode memory node
   * @param context request context
   * @return A list of saved MemChunkGroups
   */
  @Override
  public List<MemTagValueGroup> getChildren(
      MemTable memNode, InsertionRequest insertionRequest, InsertRequestContext context) {
    if (memNode.isImmutable()) return new ArrayList<>();
    List<MemTagValueGroup> memTagValueGroups = new ArrayList<>();
    String tagKey = insertionRequest.getKey(context);
    MemTagValueGroup child = memNode.get(tagKey);
    if (child != null) memTagValueGroups.add(child);
    return memTagValueGroups;
  }

  /**
   * the insert method corresponding to the MemTable node
   *
   * @param memNode memory node
   * @param context insert request context
   */
  @Override
  public void insert(
      MemTable memNode, InsertionRequest insertionRequest, InsertRequestContext context) {
    if (memNode.isImmutable()) return;
    String tagKey = insertionRequest.getKey(context);
    memNode.put(tagKey);
  }
}
