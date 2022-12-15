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
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.response.QueryResponse;
import org.apache.iotdb.lsm.annotation.QueryProcessor;
import org.apache.iotdb.lsm.context.requestcontext.QueryRequestContext;
import org.apache.iotdb.lsm.levelProcess.QueryLevelProcessor;

import org.roaringbitmap.RoaringBitmap;

import java.util.List;

/** query for MemChunk */
@QueryProcessor(level = 3)
public class MemChunkQuery extends QueryLevelProcessor<MemChunk, Object, QueryRequest> {

  /**
   * MemChunk is the last layer of memory nodes, no children
   *
   * @param memNode memory node
   * @param context request context
   * @return null
   */
  @Override
  public List<Object> getChildren(
      MemChunk memNode, QueryRequest queryRequest, QueryRequestContext context) {
    return null;
  }

  /**
   * the query method corresponding to the MemChunk node
   *
   * @param memNode memory node
   * @param context query request context
   */
  @Override
  public void query(MemChunk memNode, QueryRequest queryRequest, QueryRequestContext context) {
    QueryResponse response = context.getResponse();
    if (response == null) {
      response = new QueryResponse();
      context.setResponse(response);
    }
    RoaringBitmap roaringBitmap = context.getValue();
    if (roaringBitmap == null) roaringBitmap = new RoaringBitmap();
    RoaringBitmap now = RoaringBitmap.or(roaringBitmap, memNode.getRoaringBitmap());
    context.setValue(now);
  }
}
