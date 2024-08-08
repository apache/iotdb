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
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemChunk;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemChunkGroup;
import org.apache.iotdb.lsm.annotation.QueryProcessor;
import org.apache.iotdb.lsm.context.requestcontext.QueryRequestContext;
import org.apache.iotdb.lsm.levelProcess.QueryLevelProcessor;

import java.util.ArrayList;
import java.util.List;

/** query for MemChunkGroup */
@QueryProcessor(level = 2)
public class MemChunkGroupQuery extends QueryLevelProcessor<MemChunkGroup, MemChunk, QueryRequest> {

  /**
   * get all MemChunks that need to be processed in the current MemChunkGroup
   *
   * @param memNode memory node
   * @param context request context
   * @return A list of saved MemChunks
   */
  @Override
  public List<MemChunk> getChildren(
      MemChunkGroup memNode, QueryRequest queryRequest, QueryRequestContext context) {
    List<MemChunk> memChunks = new ArrayList<>();
    String tagValue = queryRequest.getKey(context);
    MemChunk child = memNode.get(tagValue);
    if (child != null) memChunks.add(child);
    return memChunks;
  }

  /**
   * the query method corresponding to the MemChunkGroup node
   *
   * @param memNode memory node
   * @param context query request context
   */
  @Override
  public void query(
      MemChunkGroup memNode, QueryRequest queryRequest, QueryRequestContext context) {}
}
