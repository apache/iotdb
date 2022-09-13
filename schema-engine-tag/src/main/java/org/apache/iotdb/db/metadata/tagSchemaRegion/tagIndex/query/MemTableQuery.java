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

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemChunkGroup;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTable;
import org.apache.iotdb.lsm.context.QueryContext;
import org.apache.iotdb.lsm.levelProcess.QueryLevelProcess;

import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class MemTableQuery extends QueryLevelProcess<MemTable, MemChunkGroup> {

  @Override
  public List<MemChunkGroup> getChildren(MemTable memNode, QueryContext context) {
    List<MemChunkGroup> memChunkGroups = new ArrayList<>();
    String tagKey = (String) context.getKey();
    MemChunkGroup child = memNode.get(tagKey);
    if (child != null) memChunkGroups.add(child);
    return memChunkGroups;
  }

  @Override
  public void query(MemTable memNode, QueryContext context) {
    // 如果是immutable，则需要在查询结果中删除deletionList中的id
    if (memNode.isImmutable()) {
      RoaringBitmap roaringBitmap = (RoaringBitmap) context.getResult();
      Set<Integer> deletionList = memNode.getDeletionList();
      for (Integer id : deletionList) {
        roaringBitmap.remove(id);
      }
    }
  }
}
