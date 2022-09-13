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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.deletion.DeletionManager;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.insertion.InsertionManager;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTable;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.query.QueryManager;
import org.apache.iotdb.lsm.context.DeleteContext;
import org.apache.iotdb.lsm.context.InsertContext;
import org.apache.iotdb.lsm.context.QueryContext;

import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TagInvertedIndex implements ITagInvertedIndex {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private int numOfDeviceIdsInMemTable;

  private MemTable workingMemTable;

  private MemTable unsequenceMemTable;

  private Map<Integer, MemTable> immutableMemTables;

  private int maxDeviceID;

  public TagInvertedIndex() {
    workingMemTable = new MemTable(MemTable.WORKING);
    unsequenceMemTable = new MemTable(MemTable.UNSEQUENCE);
    immutableMemTables = new HashMap<>();
    numOfDeviceIdsInMemTable = config.getNumOfDeviceIdsInMemTable();
    maxDeviceID = 0;
  }

  @Override
  public synchronized void addTags(Map<String, String> tags, int id) {
    MemTable memTable = null;
    // 出现乱序
    if (id < maxDeviceID) {
      memTable = unsequenceMemTable;
    } else {
      if (!inWorkingMemTable(id)) {
        workingMemTable.setStatus(MemTable.IMMUTABLE);
        immutableMemTables.put(maxDeviceID / numOfDeviceIdsInMemTable, workingMemTable);
        workingMemTable = new MemTable(MemTable.WORKING);
      }
      memTable = workingMemTable;
      maxDeviceID = id;
    }
    for (Map.Entry<String, String> tag : tags.entrySet()) {
      addTag(memTable, tag.getKey(), tag.getValue(), id);
    }
  }

  @Override
  public synchronized void removeTags(Map<String, String> tags, int id) {
    List<MemTable> memTables = new ArrayList<>();
    // 出现乱序
    if (inWorkingMemTable(id)) {
      memTables.add(workingMemTable);
    } else {
      memTables.add(unsequenceMemTable);
      memTables.add(immutableMemTables.get(id / numOfDeviceIdsInMemTable));
    }
    for (Map.Entry<String, String> tag : tags.entrySet()) {
      removeTag(memTables, tag.getKey(), tag.getValue(), id);
    }
  }

  @Override
  public synchronized List<Integer> getMatchedIDs(Map<String, String> tags) {
    List<MemTable> memTables = new ArrayList<>();
    memTables.add(workingMemTable);
    memTables.add(unsequenceMemTable);
    memTables.addAll(immutableMemTables.values());
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    int i = 0;
    for (Map.Entry<String, String> tag : tags.entrySet()) {
      RoaringBitmap rb = getMatchedIDs(memTables, tag.getKey(), tag.getValue());
      if (i == 0) roaringBitmap = rb;
      else roaringBitmap = RoaringBitmap.and(roaringBitmap, rb);
      i++;
    }
    return Arrays.stream(roaringBitmap.toArray()).boxed().collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return "TagInvertedIndex{"
        + "numOfDeviceIdsInMemTable="
        + numOfDeviceIdsInMemTable
        + ", workingMemTable="
        + workingMemTable
        + ", unsequenceMemTable="
        + unsequenceMemTable
        + ", immutableMemTables="
        + immutableMemTables
        + ", maxDeviceID="
        + maxDeviceID
        + '}';
  }

  private synchronized boolean inWorkingMemTable(int id) {
    return id / numOfDeviceIdsInMemTable == maxDeviceID / numOfDeviceIdsInMemTable;
  }

  private void addTag(MemTable memTable, String tagKey, String tagValue, int id) {
    InsertContext insertContext = new InsertContext(id, tagKey, tagValue);
    InsertionManager.getInstance().manager(memTable).process(insertContext);
  }

  private void removeTag(List<MemTable> memTables, String tagKey, String tagValue, int id) {
    DeleteContext deleteContext = new DeleteContext(id, tagKey, tagValue);
    for (MemTable memTable : memTables) {
      DeletionManager.getInstance().manager(memTable).process(deleteContext);
    }
  }

  private RoaringBitmap getMatchedIDs(List<MemTable> memTables, String tagKey, String tagValue) {
    QueryContext queryContext = new QueryContext(tagKey, tagValue);
    for (MemTable memTable : memTables) {
      QueryManager.getInstance().manager(memTable).process(queryContext);
    }
    return (RoaringBitmap) queryContext.getResult();
  }
}
