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

package org.apache.tsfile.file.metadata;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.controller.IChunkMetadataLoader;

import java.util.List;

public interface ITimeSeriesMetadata extends IMetadata {

  boolean isModified();

  void setModified(boolean modified);

  boolean isSeq();

  void setSeq(boolean seq);

  /**
   * Return the result has already been filtered by modification files.
   *
   * @return list of ChunkMetadata.
   */
  List<IChunkMetadata> loadChunkMetadataList();

  void setChunkMetadataLoader(IChunkMetadataLoader chunkMetadataLoader);

  /**
   * @return true if data type is matched, otherwise false
   */
  boolean typeMatch(List<TSDataType> dataTypes);
}
