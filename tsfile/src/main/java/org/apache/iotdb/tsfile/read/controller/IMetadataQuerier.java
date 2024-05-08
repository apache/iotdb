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
package org.apache.iotdb.tsfile.read.controller;

import org.apache.iotdb.tsfile.exception.write.NoMeasurementException;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.TimeRange;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface IMetadataQuerier {

  List<IChunkMetadata> getChunkMetaDataList(Path path) throws IOException;

  Map<Path, List<IChunkMetadata>> getChunkMetaDataMap(List<Path> paths) throws IOException;

  TsFileMetadata getWholeFileMetadata();

  /**
   * this will load all chunk metadata of given paths into cache.
   *
   * <p>call this method before calling getChunkMetaDataList() will accelerate the reading of chunk
   * metadata, which will only read TsMetaData once
   */
  void loadChunkMetaDatas(List<Path> paths) throws IOException;

  /**
   * @return the corresponding data type.
   * @throws NoMeasurementException if the measurement not exists.
   */
  TSDataType getDataType(Path path) throws NoMeasurementException, IOException;

  /**
   * Convert the space partition constraint to the time partition constraint.
   *
   * @param paths selected paths in a query expression
   * @param spacePartitionStartPos the start position of the space partition
   * @param spacePartitionEndPos the end position of the space partition
   * @return the converted time partition constraint
   */
  List<TimeRange> convertSpace2TimePartition(
      List<Path> paths, long spacePartitionStartPos, long spacePartitionEndPos) throws IOException;

  /** clear caches (if used) to release memory. */
  void clear();
}
