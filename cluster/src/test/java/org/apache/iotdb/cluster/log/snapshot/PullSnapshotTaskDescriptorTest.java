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

package org.apache.iotdb.cluster.log.snapshot;

import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.partition.PartitionGroup;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

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
