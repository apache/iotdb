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

package org.apache.iotdb.tool.core.model;

import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;

import java.io.Serializable;

public interface IPageInfo {
  int getUncompressedSize();

  int getCompressedSize();

  long getPosition();

  TSDataType getDataType();

  TSEncoding getEncodingType();

  CompressionType getCompressionType();

  byte getChunkType();

  Statistics<? extends Serializable> getStatistics();

  void setUncompressedSize(int uncompressedSize);

  void setCompressedSize(int compressedSize);

  void setPosition(long position);

  void setDataType(TSDataType dataType);

  void setEncodingType(TSEncoding encodingType);

  void setCompressionType(CompressionType compressionType);

  void setChunkType(byte chunkType);

  void setStatistics(Statistics<? extends Serializable> statistics);
}
