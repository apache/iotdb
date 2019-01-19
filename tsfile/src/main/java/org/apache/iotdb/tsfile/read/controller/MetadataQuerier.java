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
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.common.Path;

public interface MetadataQuerier {

  List<ChunkMetaData> getChunkMetaDataList(Path path) throws IOException;

  TsFileMetaData getWholeFileMetadata();

  /**
   * this will load all chunk metadata of given paths into cache.
   *
   * <p>call this method before calling getChunkMetaDataList() will accelerate the reading of chunk
   * metadata, which will only read TsDeviceMetaData once
   */
  void loadChunkMetaDatas(List<Path> paths) throws IOException;

}
