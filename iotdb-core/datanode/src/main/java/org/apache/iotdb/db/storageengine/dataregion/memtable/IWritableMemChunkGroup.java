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

package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryValue;

import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.List;
import java.util.Map;

public interface IWritableMemChunkGroup extends WALEntryValue {

  boolean writeValuesWithFlushCheck(
      long[] times,
      Object[] columns,
      BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList,
      int start,
      int end,
      TSStatus[] results);

  void release();

  long count();

  boolean contains(String measurement);

  boolean writeWithFlushCheck(
      long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList);

  Map<String, IWritableMemChunk> getMemChunkMap();

  int delete(
      PartialPath originalPath, PartialPath devicePath, long startTimestamp, long endTimestamp);

  long getCurrentTVListSize(String measurement);

  long getMaxTime();
}
