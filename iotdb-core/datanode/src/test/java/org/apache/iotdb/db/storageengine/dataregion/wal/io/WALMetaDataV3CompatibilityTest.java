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
package org.apache.iotdb.db.storageengine.dataregion.wal.io;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for WALMetaData V3 serialization/deserialization roundtrip and V2->V3 compatibility. */
public class WALMetaDataV3CompatibilityTest {

  @Test
  public void testV3RoundTrip() {
    WALMetaData original = new WALMetaData();

    original.add(100, 10, 1, 10000L, 1, 2L, 10L);
    original.add(200, 11, 1, 10010L, 1, 2L, 11L);
    original.add(150, 12, 1, 10020L, 1, 2L, 12L);
    original.add(300, 13, 2, 20000L, 4, 1L, 1L);
    original.add(250, 14, 2, 20010L, 1, 2L, 14L);

    original.updateTimestampRange(1600000000000L);
    original.updateTimestampRange(1600000001000L);

    int size = original.serializedSize(WALFileVersion.V3);
    ByteBuffer buffer = ByteBuffer.allocate(size);
    original.serialize(buffer, WALFileVersion.V3);
    buffer.flip();

    WALMetaData deserialized = WALMetaData.deserialize(buffer, WALFileVersion.V3);

    assertEquals(10, deserialized.getFirstSearchIndex());
    assertEquals(5, deserialized.getBuffersSize().size());
    assertEquals(Integer.valueOf(100), deserialized.getBuffersSize().get(0));
    assertEquals(Integer.valueOf(200), deserialized.getBuffersSize().get(1));
    assertEquals(Integer.valueOf(150), deserialized.getBuffersSize().get(2));
    assertEquals(Integer.valueOf(300), deserialized.getBuffersSize().get(3));
    assertEquals(Integer.valueOf(250), deserialized.getBuffersSize().get(4));

    assertTrue(deserialized.getMemTablesId().contains(1L));
    assertTrue(deserialized.getMemTablesId().contains(2L));

    assertEquals(1600000000000L, deserialized.getMinDataTs());
    assertEquals(1600000001000L, deserialized.getMaxDataTs());

    assertEquals(5, deserialized.getPhysicalTimes().size());
    assertEquals(Long.valueOf(10000L), deserialized.getPhysicalTimes().get(0));
    assertEquals(Long.valueOf(10010L), deserialized.getPhysicalTimes().get(1));
    assertEquals(Long.valueOf(10020L), deserialized.getPhysicalTimes().get(2));
    assertEquals(Long.valueOf(20000L), deserialized.getPhysicalTimes().get(3));
    assertEquals(Long.valueOf(20010L), deserialized.getPhysicalTimes().get(4));

    assertEquals(5, deserialized.getNodeIds().size());
    assertEquals(Short.valueOf((short) 1), deserialized.getNodeIds().get(0));
    assertEquals(Short.valueOf((short) 1), deserialized.getNodeIds().get(1));
    assertEquals(Short.valueOf((short) 1), deserialized.getNodeIds().get(2));
    assertEquals(Short.valueOf((short) 4), deserialized.getNodeIds().get(3));
    assertEquals(Short.valueOf((short) 1), deserialized.getNodeIds().get(4));

    assertEquals(5, deserialized.getWriterEpochs().size());
    assertEquals(Short.valueOf((short) 2), deserialized.getWriterEpochs().get(0));
    assertEquals(Short.valueOf((short) 2), deserialized.getWriterEpochs().get(1));
    assertEquals(Short.valueOf((short) 2), deserialized.getWriterEpochs().get(2));
    assertEquals(Short.valueOf((short) 1), deserialized.getWriterEpochs().get(3));
    assertEquals(Short.valueOf((short) 2), deserialized.getWriterEpochs().get(4));

    assertEquals(5, deserialized.getLocalSeqs().size());
    assertEquals(Long.valueOf(10L), deserialized.getLocalSeqs().get(0));
    assertEquals(Long.valueOf(11L), deserialized.getLocalSeqs().get(1));
    assertEquals(Long.valueOf(12L), deserialized.getLocalSeqs().get(2));
    assertEquals(Long.valueOf(1L), deserialized.getLocalSeqs().get(3));
    assertEquals(Long.valueOf(14L), deserialized.getLocalSeqs().get(4));
  }

  @Test
  public void testV2DeserializationHasEmptyV3Fields() {
    WALMetaData original = new WALMetaData();
    original.add(100, 10, 1, 1000L, 10);
    original.add(200, 11, 1, 2000L, 11);

    int size = original.serializedSize(WALFileVersion.V2);
    ByteBuffer buffer = ByteBuffer.allocate(size);
    original.serialize(buffer, WALFileVersion.V2);
    buffer.flip();

    WALMetaData deserialized = WALMetaData.deserialize(buffer, WALFileVersion.V2);

    assertEquals(10, deserialized.getFirstSearchIndex());
    assertEquals(2, deserialized.getBuffersSize().size());
    assertTrue(deserialized.getPhysicalTimes().isEmpty());
    assertTrue(deserialized.getNodeIds().isEmpty());
    assertTrue(deserialized.getWriterEpochs().isEmpty());
    assertTrue(deserialized.getLocalSeqs().isEmpty());
    assertEquals(Long.MAX_VALUE, deserialized.getMinDataTs());
    assertEquals(Long.MIN_VALUE, deserialized.getMaxDataTs());
  }

  @Test
  public void testV2SerializedSizeSmallerThanV3() {
    WALMetaData meta = new WALMetaData();
    meta.add(100, 10, 1, 10L, 1, 2L, 10L);
    meta.add(200, 11, 1, 11L, 1, 2L, 11L);
    meta.add(300, 12, 1, 12L, 3, 1L, 12L);

    int v2Size = meta.serializedSize(WALFileVersion.V2);
    int v3Size = meta.serializedSize(WALFileVersion.V3);

    int entryCount = 3;
    int overrideCount = 1;
    int expectedDiff =
        entryCount * Long.BYTES * 2
            + Long.BYTES * 2
            + Short.BYTES * 2
            + Integer.BYTES
            + overrideCount * (Integer.BYTES + Short.BYTES + Short.BYTES);
    assertEquals(expectedDiff, v3Size - v2Size);
  }

  @Test
  public void testV3AddAllMerge() {
    WALMetaData meta1 = new WALMetaData();
    meta1.add(100, 10, 1, 100L, 1, 2L, 10L);
    meta1.add(200, 11, 1, 110L, 1, 2L, 11L);
    meta1.updateTimestampRange(100L);

    WALMetaData meta2 = new WALMetaData();
    meta2.add(300, 12, 2, 200L, 4, 1L, 1L);
    meta2.updateTimestampRange(200L);

    meta1.addAll(meta2);

    assertEquals(3, meta1.getBuffersSize().size());
    assertEquals(Long.valueOf(100L), meta1.getPhysicalTimes().get(0));
    assertEquals(Long.valueOf(110L), meta1.getPhysicalTimes().get(1));
    assertEquals(Long.valueOf(200L), meta1.getPhysicalTimes().get(2));
    assertEquals(Short.valueOf((short) 1), meta1.getNodeIds().get(0));
    assertEquals(Short.valueOf((short) 1), meta1.getNodeIds().get(1));
    assertEquals(Short.valueOf((short) 4), meta1.getNodeIds().get(2));
    assertEquals(Short.valueOf((short) 2), meta1.getWriterEpochs().get(0));
    assertEquals(Short.valueOf((short) 2), meta1.getWriterEpochs().get(1));
    assertEquals(Short.valueOf((short) 1), meta1.getWriterEpochs().get(2));
    assertEquals(Long.valueOf(10L), meta1.getLocalSeqs().get(0));
    assertEquals(Long.valueOf(11L), meta1.getLocalSeqs().get(1));
    assertEquals(Long.valueOf(1L), meta1.getLocalSeqs().get(2));
    assertEquals(100L, meta1.getMinDataTs());
    assertEquals(200L, meta1.getMaxDataTs());
  }

  @Test
  public void testV3EmptyMetadata() {
    WALMetaData empty = new WALMetaData();

    int size = empty.serializedSize(WALFileVersion.V3);
    ByteBuffer buffer = ByteBuffer.allocate(size);
    empty.serialize(buffer, WALFileVersion.V3);
    buffer.flip();

    WALMetaData deserialized = WALMetaData.deserialize(buffer, WALFileVersion.V3);

    assertEquals(0, deserialized.getBuffersSize().size());
    assertTrue(deserialized.getPhysicalTimes().isEmpty());
    assertTrue(deserialized.getNodeIds().isEmpty());
    assertTrue(deserialized.getWriterEpochs().isEmpty());
    assertTrue(deserialized.getLocalSeqs().isEmpty());
    assertEquals(Long.MAX_VALUE, deserialized.getMinDataTs());
    assertEquals(Long.MIN_VALUE, deserialized.getMaxDataTs());
  }

  @Test
  public void testV2CompatibleAddDefaultsWriterProgress() {
    WALMetaData meta = new WALMetaData();
    meta.add(100, 10, 1);

    assertEquals(Long.valueOf(0L), meta.getPhysicalTimes().get(0));
    assertEquals(Short.valueOf((short) -1), meta.getNodeIds().get(0));
    assertEquals(Short.valueOf((short) 0), meta.getWriterEpochs().get(0));
    assertEquals(Long.valueOf(10L), meta.getLocalSeqs().get(0));
  }
}
