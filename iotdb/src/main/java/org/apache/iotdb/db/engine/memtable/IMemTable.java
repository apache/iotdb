/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.engine.memtable;

import java.util.Map;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

/**
 * IMemTable is designed to store data points which are not flushed into TsFile yet. An instance of
 * IMemTable maintains all series belonging to one StorageGroup,
 * corresponding to one FileNodeProcessor.<br> The concurrent control of IMemTable
 * is based on the concurrent control of FileNodeProcessor, i.e., Writing and
 * querying operations must already have gotten writeLock and readLock respectively.<br>
 */
public interface IMemTable {

  Map<String, Map<String, IWritableMemChunk>> getMemTableMap();

  void write(String deviceId, String measurement, TSDataType dataType,
      long insertTime, String insertValue);

  int size();

  TimeValuePairSorter query(String deviceId, String measurement, TSDataType dataType);

  /**
   * release all the memory resources.
   */
  void clear();

  boolean isEmpty();

}
