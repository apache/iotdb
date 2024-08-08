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

package org.apache.iotdb.consensus.iot.snapshot;

import org.apache.iotdb.consensus.iot.thrift.TSendSnapshotFragmentReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SnapshotFragment {

  private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotFragment.class);

  private final String snapshotId;
  private final String filePath;
  private final long totalSize;
  private final long startOffset;
  private final long fragmentSize;
  private final ByteBuffer fileChunk;
  private final String md5;

  public SnapshotFragment(
      String snapshotId,
      String filePath,
      long totalSize,
      long startOffset,
      long fragmentSize,
      ByteBuffer fileChunk) {
    this.snapshotId = snapshotId;
    this.filePath = filePath;
    this.totalSize = totalSize;
    this.startOffset = startOffset;
    this.fragmentSize = fragmentSize;
    this.fileChunk = fileChunk;
    this.md5 = calculateMd5(fileChunk);
  }

  public static String calculateMd5(ByteBuffer byteBuffer) {
    MessageDigest messageDigest;
    try {
      messageDigest = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      LOGGER.warn(
          "Cannot find MD5 algorithm, the correctness of snapshot file chunk won't be verified.");
      return "";
    }
    byteBuffer.mark();
    messageDigest.update(byteBuffer);
    byteBuffer.reset();
    return new BigInteger(1, messageDigest.digest()).toString(16);
  }

  public TSendSnapshotFragmentReq toTSendSnapshotFragmentReq() {
    TSendSnapshotFragmentReq req = new TSendSnapshotFragmentReq();
    req.setSnapshotId(snapshotId);
    req.setFilePath(filePath);
    req.setChunkLength(fragmentSize);
    req.setFileChunk(fileChunk);
    req.setFileChunkMD5(md5);
    return req;
  }

  public String getSnapshotId() {
    return snapshotId;
  }

  public String getFilePath() {
    return filePath;
  }

  public long getTotalSize() {
    return totalSize;
  }

  public long getStartOffset() {
    return startOffset;
  }

  public long getFragmentSize() {
    return fragmentSize;
  }

  public ByteBuffer getFileChunk() {
    return fileChunk;
  }
}
