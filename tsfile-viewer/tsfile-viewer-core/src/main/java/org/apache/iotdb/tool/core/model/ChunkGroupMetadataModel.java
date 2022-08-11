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
package org.apache.iotdb.tool.core.model;

import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;

import java.util.List;

/** Only maintained when writing, not serialized to TsFile */
public class ChunkGroupMetadataModel {

  private String device;

  private List<IChunkMetadata> chunkMetadataList;

  private List<List<ChunkHeader>> chunkHeaderLists;

  public ChunkGroupMetadataModel(
      String device,
      List<IChunkMetadata> chunkMetadataList,
      List<List<ChunkHeader>> chunkHeaderList) {
    this.device = device;
    this.chunkMetadataList = chunkMetadataList;
    this.chunkHeaderLists = chunkHeaderList;
  }

  public String getDevice() {
    return device;
  }

  public List<IChunkMetadata> getChunkMetadataList() {
    return chunkMetadataList;
  }

  public List<List<ChunkHeader>> getChunkHeaderLists() {
    return chunkHeaderLists;
  }
}
