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
import java.util.List;

public class AlignedPageInfo implements IPageInfo {
  private IPageInfo timePageInfo;
  private List<IPageInfo> valuePageInfoList;

  @Override
  public int getUncompressedSize() {
    return this.timePageInfo.getUncompressedSize();
  }

  @Override
  public int getCompressedSize() {
    return this.timePageInfo.getCompressedSize();
  }

  @Override
  public long getPosition() {
    return this.timePageInfo.getPosition();
  }

  @Override
  public TSDataType getDataType() {
    return this.timePageInfo.getDataType();
  }

  @Override
  public TSEncoding getEncodingType() {
    return this.timePageInfo.getEncodingType();
  }

  @Override
  public CompressionType getCompressionType() {
    return this.timePageInfo.getCompressionType();
  }

  @Override
  public byte getChunkType() {
    return this.timePageInfo.getChunkType();
  }

  @Override
  public Statistics<? extends Serializable> getStatistics() {
    return this.timePageInfo.getStatistics();
  }

  @Override
  public void setUncompressedSize(int uncompressedSize) {
    this.timePageInfo.setUncompressedSize(uncompressedSize);
  }

  @Override
  public void setCompressedSize(int compressedSize) {
    this.timePageInfo.setCompressedSize(compressedSize);
  }

  @Override
  public void setPosition(long position) {
    this.timePageInfo.setPosition(position);
  }

  @Override
  public void setDataType(TSDataType dataType) {
    this.timePageInfo.setDataType(dataType);
  }

  @Override
  public void setEncodingType(TSEncoding encodingType) {
    this.timePageInfo.setEncodingType(encodingType);
  }

  @Override
  public void setCompressionType(CompressionType compressionType) {
    this.timePageInfo.setCompressionType(compressionType);
  }

  @Override
  public void setChunkType(byte chunkType) {
    this.timePageInfo.setChunkType(chunkType);
  }

  @Override
  public void setStatistics(Statistics<? extends Serializable> statistics) {
    this.timePageInfo.setStatistics(statistics);
  }

  public IPageInfo getTimePageInfo() {
    return timePageInfo;
  }

  public void setTimePageInfo(IPageInfo timePageInfo) {
    this.timePageInfo = timePageInfo;
  }

  public List<IPageInfo> getValuePageInfoList() {
    return valuePageInfoList;
  }

  public void setValuePageInfoList(List<IPageInfo> valuePageInfoList) {
    this.valuePageInfoList = valuePageInfoList;
  }
}
