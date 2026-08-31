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
 */
package org.apache.iotdb.db.storageengine.load.splitter;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;

import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;

/** Physical Chunk bytes and the metadata required to seal the TsFile footer. */
public final class EncodedChunk {
  private final IDeviceID device;
  private final TTimePartitionSlot timePartitionSlot;
  private final byte[] bytes;
  private final IChunkMetadata metadata;
  private long offset;

  public EncodedChunk(
      IDeviceID device,
      TTimePartitionSlot timePartitionSlot,
      byte[] bytes,
      IChunkMetadata metadata) {
    this.device = device;
    this.timePartitionSlot = timePartitionSlot;
    this.bytes = bytes;
    this.metadata = metadata;
    this.offset = 0;
  }

  public IDeviceID getDevice() {
    return device;
  }

  public TTimePartitionSlot getTimePartitionSlot() {
    return timePartitionSlot;
  }

  public byte[] getBytes() {
    return bytes;
  }

  public IChunkMetadata getMetadata() {
    return metadata;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }
}
