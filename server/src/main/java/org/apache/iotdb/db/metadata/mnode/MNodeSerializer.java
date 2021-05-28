package org.apache.iotdb.db.metadata.mnode;

import java.nio.ByteBuffer;

public interface MNodeSerializer {

    ByteBuffer serializeInternalMNode(InternalMNode mNode);

    InternalMNode deserializeInternalMNode(ByteBuffer dataBuffer);

    ByteBuffer serializeStorageGroupMNode(StorageGroupMNode mNode);

    StorageGroupMNode deserializeStorageGroupMNode(ByteBuffer dataBuffer);

    ByteBuffer serializeMeasurementMNode(MeasurementMNode mNode);

    MeasurementMNode deserializeMeasurementMNode(ByteBuffer dataBuffer);

    int evaluateMNodeLength(MNode mNode);

}
