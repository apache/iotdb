/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.iotdb.db.storageengine.load.splitter;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public final class EncodedChunkGroup implements TsFileData {
  private final IDeviceID device;
  private final TTimePartitionSlot timePartitionSlot;
  private final List<EncodedChunk> chunks;

  public EncodedChunkGroup(
      IDeviceID device, TTimePartitionSlot timePartitionSlot, List<EncodedChunk> chunks) {
    this.device = device;
    this.timePartitionSlot = timePartitionSlot;
    this.chunks = List.copyOf(chunks);
  }

  public IDeviceID getDevice() {
    return device;
  }

  public TTimePartitionSlot getTimePartitionSlot() {
    return timePartitionSlot;
  }

  public List<EncodedChunk> getChunks() {
    return chunks;
  }

  @Override
  public long getDataSize() {
    return chunks.stream().mapToLong(chunk -> chunk.getBytes().length).sum();
  }

  @Override
  public TsFileDataType getType() {
    return TsFileDataType.ENCODED_CHUNK;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(getType().ordinal(), stream);
    ReadWriteIOUtils.write(timePartitionSlot.getStartTime(), stream);
    ReadWriteIOUtils.write(device instanceof StringArrayDeviceID, stream);
    device.serialize(stream);
    ReadWriteIOUtils.write(chunks.size(), stream);
    for (EncodedChunk chunk : chunks) {
      ReadWriteIOUtils.write(chunk.getMetadata().getMeasurementUid(), stream);
      ReadWriteIOUtils.write(chunk.getMetadata().getDataType().ordinal(), stream);
      ReadWriteIOUtils.write(chunk.getOffset(), stream);
      ReadWriteIOUtils.write(chunk.getBytes().length, stream);
      stream.write(chunk.getBytes());
      chunk.getMetadata().serializeTo(stream, true);
    }
  }
}
