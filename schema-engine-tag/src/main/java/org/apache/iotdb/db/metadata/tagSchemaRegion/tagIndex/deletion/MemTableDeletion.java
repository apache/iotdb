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

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemChunkGroup;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTable;
import org.apache.iotdb.lsm.context.DeleteContext;
import org.apache.iotdb.lsm.levelProcess.DeleteLevelProcess;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class MemTableDeletion extends DeleteLevelProcess<MemTable, MemChunkGroup> {
  @Override
  public List<MemChunkGroup> getChildren(MemTable memNode, DeleteContext context) {
    if (memNode.isImmutable()) return new ArrayList<>();
    List<MemChunkGroup> memChunkGroups = new ArrayList<>();
    String tagKey = (String) context.getKey();
    MemChunkGroup child = memNode.get(tagKey);
    if (child != null) memChunkGroups.add(child);
    return memChunkGroups;
  }

  @Override
  public void delete(MemTable memNode, DeleteContext context) {
    if (memNode.isImmutable()) {
      Set<Integer> deletionList = memNode.getDeletionList();
      if (!deletionList.contains(context.getValue())) {
        deletionList.add((Integer) context.getValue());
      }
      return;
    }
    String tagKey = (String) context.getKey();
    MemChunkGroup child = memNode.get(tagKey);
    if (child == null || child.isEmpty()) {
      memNode.remove(tagKey);
    }
  }
}
