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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** used to manage tagKey -> MemChunkGroup */
public class MemTable {

  public static final String WORKING = "working";

  public static final String IMMUTABLE = "immutable";

  // manage tagKey -> MemChunkGroup
  private Map<String, MemChunkGroup> memChunkGroupMap;

  private String status;

  // if the memTable is immutable, the data cannot be deleted directly, and the deleted data needs
  // to be recorded in the deletionList
  private Set<Integer> deletionList;

  public MemTable(String status) {
    memChunkGroupMap = new HashMap<>();
    this.status = status;
    deletionList = new HashSet<>();
  }

  public void put(String tagKey) {
    if (this.status.equals(IMMUTABLE)) return;
    if (!memChunkGroupMap.containsKey(tagKey)) {
      memChunkGroupMap.put(tagKey, new MemChunkGroup());
    }
  }

  @Override
  public String toString() {
    return "MemTable{"
        + "memChunkGroupMap="
        + memChunkGroupMap
        + ", status='"
        + status
        + '\''
        + ", deletionList="
        + deletionList
        + '}';
  }

  public MemChunkGroup get(String tagKey) {
    return memChunkGroupMap.get(tagKey);
  }

  public void remove(String tagKey) {
    memChunkGroupMap.remove(tagKey);
  }

  public boolean isImmutable() {
    return status.equals(IMMUTABLE);
  }

  public Set<Integer> getDeletionList() {
    return deletionList;
  }

  public void setStatus(String status) {
    this.status = status;
  }
}
