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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.query;

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.Request.QueryRequest;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemChunkGroup;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTable;
import org.apache.iotdb.lsm.annotation.QueryProcessor;
import org.apache.iotdb.lsm.context.requestcontext.QueryRequestContext;
import org.apache.iotdb.lsm.levelProcess.QueryLevelProcessor;

import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** query for MemTable */
@QueryProcessor(level = 1)
public class MemTableQuery extends QueryLevelProcessor<MemTable, MemChunkGroup, QueryRequest> {

  /**
   * get all MemChunkGroups that need to be processed in the current MemTable
   *
   * @param memNode memory node
   * @param context request context
   * @return A list of saved MemChunkGroups
   */
  @Override
  public List<MemChunkGroup> getChildren(
      MemTable memNode, QueryRequest queryRequest, QueryRequestContext context) {
    List<MemChunkGroup> memChunkGroups = new ArrayList<>();
    String tagKey = queryRequest.getKey(context);
    MemChunkGroup child = memNode.get(tagKey);
    if (child != null) memChunkGroups.add(child);
    return memChunkGroups;
  }

  /**
   * the query method corresponding to the MemTable node
   *
   * @param memNode memory node
   * @param context query request context
   */
  @Override
  public void query(MemTable memNode, QueryRequest queryRequest, QueryRequestContext context) {
    // if the memTable is immutable, we need to delete the id in deletionList in the query result
    if (memNode.isImmutable()) {
      RoaringBitmap roaringBitmap = context.getValue();
      Set<Integer> deletionList = memNode.getDeletionList();
      for (Integer id : deletionList) {
        roaringBitmap.remove(id);
      }
    }
  }
}
