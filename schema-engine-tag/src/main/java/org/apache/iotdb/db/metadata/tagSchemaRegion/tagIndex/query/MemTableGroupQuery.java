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
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTable;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTableGroup;
import org.apache.iotdb.lsm.context.QueryRequestContext;
import org.apache.iotdb.lsm.levelProcess.QueryLevelProcess;

import java.util.ArrayList;
import java.util.List;

public class MemTableGroupQuery extends QueryLevelProcess<MemTableGroup, MemTable, QueryRequest> {
  @Override
  public List<MemTable> getChildren(
      MemTableGroup memNode, QueryRequest request, QueryRequestContext context) {
    List<MemTable> memTables = new ArrayList<>();
    memTables.add(memNode.getWorkingMemTable());
    memTables.addAll(memNode.getImmutableMemTables().values());
    return memTables;
  }

  @Override
  public void query(MemTableGroup memNode, QueryRequest request, QueryRequestContext context) {}
}
