/**
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.tsfile.exception.write.NoMeasurementException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.TimeRange;

public interface MetadataQuerier {

  List<ChunkMetaData> getChunkMetaDataList(Path path) throws IOException;

  Map<Path, List<ChunkMetaData>> getChunkMetaDataMap(List<Path> paths) throws IOException;

  TsFileMetaData getWholeFileMetadata();

  /**
   * this will load all chunk metadata of given paths into cache.
   *
   * <p>call this method before calling getChunkMetaDataList() will accelerate the reading of chunk
   * metadata, which will only read TsDeviceMetaData once
   */
  void loadChunkMetaDatas(List<Path> paths) throws IOException;

  /**
   *
   * @param measurement
   * @return the corresponding data type.
   * @throws NoMeasurementException if the measurement not exists.
   */
  TSDataType getDataType(String measurement) throws NoMeasurementException;

  /**
   * get time ranges of chunkGroups in or before the current partition and return the union result in ascending order
   *
   * @param paths timeseries paths
   * @param targetMode InPartition or PrevPartition
   * @return time ranges union in ascending order
   * @throws IOException
   */
  public ArrayList<TimeRange> getTimeRangeInOrPrev(List<Path> paths, LoadMode targetMode)
      throws IOException;

  /**
   * get the load mode of the MetadataQuerier
   *
   * @return LoadMode enum
   */
  LoadMode getLoadMode();

  /**
   * set the load mode of the MetadataQuerier
   *
   * @param mode enum
   */
  void setLoadMode(LoadMode mode);

  /**
   * The load mode of the MetadataQuerier:
   * NoPartition - load metadata of all chunkgroups in the file
   * InPartition - load metadata of chunkgroups which fall in the current partition
   * PrevPartition - load metadata of chunkgroups which fall ahead the current partition
   */
  enum LoadMode {
    NoPartition, InPartition, PrevPartition
  }
}
