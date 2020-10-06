package org.apache.iotdb.cluster.log.snapshot;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.junit.Test;

public class PullSnapshotTaskDescriptorTest {

  @Test
  public void testSerialize() throws IOException {
    PartitionGroup group = new PartitionGroup();
    List<Integer> slots = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      group.add(TestUtils.getNode(i));
      slots.add(i);
    }

    PullSnapshotTaskDescriptor descriptor = new PullSnapshotTaskDescriptor(group, slots, true);

    byte[] bytes;
    try (ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(arrayOutputStream)) {
      descriptor.serialize(dataOutputStream);
      bytes = arrayOutputStream.toByteArray();
    }

    try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream)) {
      PullSnapshotTaskDescriptor deserialized = new PullSnapshotTaskDescriptor();
      deserialized.deserialize(dataInputStream);
      assertEquals(descriptor, deserialized);
    }
  }
}