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
package org.apache.iotdb.db.engine.memtable;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.db.utils.datastructure.TVSkipListMap;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

public interface IWritableMemChunk extends TimeValuePairSorter {

  void put(long t, Object v);

  void puts(long[] t, Object[] v);

  void write(long insertTime, Object objectValue);

  void write(long[] times, Object valueList, TSDataType dataType, List<Integer> indexes);

  long count();

  TSDataType getType();

  /**
   * using offset to mark which data is deleted: the data whose timestamp is less than offset are
   * deleted.
   *
   * @param offset
   */
  void setTimeOffset(long offset);

  /**
   * served for query requests.
   *
   * @return
   */
  TVSkipListMap<Long, TimeValuePair> getDataList();
//  default TVList getSortedTVList(){return null;}
//
//  default TVList getTVList(){return null;}

  default long getMinTime() {
    return Long.MIN_VALUE;
  }

  void delete(long upperBound);
}
