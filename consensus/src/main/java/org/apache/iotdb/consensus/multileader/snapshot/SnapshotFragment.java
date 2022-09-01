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

package org.apache.iotdb.consensus.multileader.snapshot;

import org.apache.iotdb.consensus.multileader.thrift.TSendSnapshotFragmentReq;

import java.nio.ByteBuffer;

public class SnapshotFragment {
  private String snapshotId;
  private String filePath;
  private long totalSize;
  private long startOffset;
  private long fragmentSize;
  private ByteBuffer fileChunk;

  public String getSnapshotId() {
    return snapshotId;
  }

  public void setSnapshotId(String snapshotId) {
    this.snapshotId = snapshotId;
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public long getTotalSize() {
    return totalSize;
  }

  public void setTotalSize(long totalSize) {
    this.totalSize = totalSize;
  }

  public long getStartOffset() {
    return startOffset;
  }

  public void setStartOffset(long startOffset) {
    this.startOffset = startOffset;
  }

  public long getFragmentSize() {
    return fragmentSize;
  }

  public void setFragmentSize(long fragmentSize) {
    this.fragmentSize = fragmentSize;
  }

  public ByteBuffer getFileChunk() {
    return fileChunk;
  }

  public void setFileChunk(ByteBuffer fileChunk) {
    this.fileChunk = fileChunk;
  }

  public TSendSnapshotFragmentReq toTSendSnapshotFragmentReq() {
    TSendSnapshotFragmentReq req = new TSendSnapshotFragmentReq();
    req.setSnapshotId(snapshotId);
    req.setFilePath(filePath);
    req.setChunkLength(fragmentSize);
    req.setFileChunk(fileChunk);
    return req;
  }
}
