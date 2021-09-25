package org.apache.iotdb.db.metadata.mnode;

import java.nio.ByteBuffer;

public interface IMNodeSerializer {

  int INTERNAL_MNODE = 0;
  int STORAGE_GROUP_MNODE = 1;
  int MEASUREMENT_MNODE = 2;

  ByteBuffer serializeMNode(IMNode mNode);

  void serializeMNode(IMNode mNode, ByteBuffer dataBuffer);

  IMNode deserializeMNode(ByteBuffer dataBuffer, int type);

  ByteBuffer serializeInternalMNode(InternalMNode mNode);

  void serializeInternalMNode(InternalMNode mNode, ByteBuffer dataBuffer);

  InternalMNode deserializeInternalMNode(ByteBuffer dataBuffer);

  ByteBuffer serializeStorageGroupMNode(StorageGroupMNode mNode);

  void serializeStorageGroupMNode(StorageGroupMNode mNode, ByteBuffer dataBuffer);

  StorageGroupMNode deserializeStorageGroupMNode(ByteBuffer dataBuffer);

  ByteBuffer serializeMeasurementMNode(MeasurementMNode mNode);

  void serializeMeasurementMNode(MeasurementMNode mNode, ByteBuffer dataBuffer);

  MeasurementMNode deserializeMeasurementMNode(ByteBuffer dataBuffer);

  int evaluateMNodeLength(IMNode mNode);
}
