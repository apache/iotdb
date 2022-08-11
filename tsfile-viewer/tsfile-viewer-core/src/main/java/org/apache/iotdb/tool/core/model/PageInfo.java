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

public class PageInfo implements IPageInfo {

  private int uncompressedSize;

  private int compressedSize;

  private long position;

  private TSDataType dataType;

  private TSEncoding encodingType;

  private CompressionType compressionType;

  private byte chunkType;

  private Statistics<? extends Serializable> statistics;

  public PageInfo() {}

  public PageInfo(long position) {
    this.position = position;
  }

  public int getUncompressedSize() {
    return uncompressedSize;
  }

  public int getCompressedSize() {
    return compressedSize;
  }

  public long getPosition() {
    return position;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public TSEncoding getEncodingType() {
    return encodingType;
  }

  public CompressionType getCompressionType() {
    return compressionType;
  }

  public byte getChunkType() {
    return chunkType;
  }

  public Statistics<? extends Serializable> getStatistics() {
    return statistics;
  }

  public void setUncompressedSize(int uncompressedSize) {
    this.uncompressedSize = uncompressedSize;
  }

  public void setCompressedSize(int compressedSize) {
    this.compressedSize = compressedSize;
  }

  public void setPosition(long position) {
    this.position = position;
  }

  public void setDataType(TSDataType dataType) {
    this.dataType = dataType;
  }

  public void setEncodingType(TSEncoding encodingType) {
    this.encodingType = encodingType;
  }

  public void setCompressionType(CompressionType compressionType) {
    this.compressionType = compressionType;
  }

  public void setChunkType(byte chunkType) {
    this.chunkType = chunkType;
  }

  public void setStatistics(Statistics<? extends Serializable> statistics) {
    this.statistics = statistics;
  }

  @Override
  public String toString() {
    return "PageInfo{"
        + "uncompressedSize="
        + uncompressedSize
        + ", compressedSize="
        + compressedSize
        + ", position="
        + position
        + ", dataType="
        + dataType
        + ", encodingType="
        + encodingType
        + ", compressionType="
        + compressionType
        + ", chunkType="
        + chunkType
        + ", statistics="
        + statistics
        + '}';
  }
}
