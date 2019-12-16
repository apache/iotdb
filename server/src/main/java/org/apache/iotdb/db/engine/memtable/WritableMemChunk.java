/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.memtable;

import java.util.Collection;
import java.util.List;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TsPrimitiveType;
import org.apache.iotdb.db.utils.datastructure.TVSkipListMap;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WritableMemChunk implements IWritableMemChunk {

  private static final Logger logger = LoggerFactory.getLogger(WritableMemChunk.class);
  private TSDataType dataType;
  private TVSkipListMap<Long, TimeValuePair> list;

  public WritableMemChunk(TSDataType dataType, TVSkipListMap list) {
    this.dataType = dataType;
    this.list = list;
  }

  @Override
  public void write(long insertTime, Object objectValue) {
    put(insertTime, objectValue);
  }

  @Override
  public void write(long[] times, Object valueList, TSDataType dataType, List<Integer> indexes) {
    Object[] objects = (Object[]) valueList;
    if (times.length == indexes.size()) {
      puts(times, objects);
    }
    for (Integer index : indexes) {
      put(times[index], objects[index]);
    }
  }


  @Override
  public void put(long t, Object v) {
    list.put(t, new TimeValuePair(t, TsPrimitiveType.getByType(dataType, v)));
  }

  @Override
  public void puts(long[] t, Object[] v) {
    for (int i = 0; i < t.length; i++) {
      put(t[i], v[i]);
    }
  }

  @Override
  public long count() {
    return list.size();
  }

  @Override
  public TSDataType getType() {
    return dataType;
  }

  @Override
  public void setTimeOffset(long offset) {
    list.setTimeOffset(offset);
  }

  @Override
  public TVSkipListMap<Long, TimeValuePair> getDataList() {
    return list;
  }

  @Override
  public synchronized Collection<TimeValuePair> getSortedTimeValuePairList() {
    return this.list.values();
  }

  @Override
  public boolean isEmpty() {
    return list.size() == 0;
  }

  @Override
  public long getMinTime() {
    return list.firstKey();
  }

  @Override
  public void delete(long upperBound) {
    list.remove(upperBound);
  }
}
