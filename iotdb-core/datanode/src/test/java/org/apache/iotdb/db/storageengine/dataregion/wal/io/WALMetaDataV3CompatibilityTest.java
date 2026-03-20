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

/**
 * Tests for WALMetaData V3 serialization/deserialization roundtrip and V2→V3 compatibility.
 *
 * <p>V3 extends V2 by adding per-entry epoch[] and syncIndex[] arrays, plus file-level (minDataTs,
 * maxDataTs) for ordered consensus subscription.
 */
public class WALMetaDataV3CompatibilityTest {

  @Test
  public void testV3RoundTrip() {
    // Build V3 metadata with multiple entries of different epochs
    WALMetaData original = new WALMetaData();

    // Simulate 5 entries: 3 from epoch 1000, 2 from epoch 2000
    original.add(100, /*searchIndex*/ 10, /*memTableId*/ 1, /*epoch*/ 1000L, /*syncIndex*/ 10);
    original.add(200, 11, 1, 1000L, 11);
    original.add(150, 12, 1, 1000L, 12);
    original.add(300, 13, 2, 2000L, 1);
    original.add(250, 14, 2, 2000L, 2);

    original.updateTimestampRange(1600000000000L);
    original.updateTimestampRange(1600000001000L);

    // Serialize as V3
    int size = original.serializedSize(WALFileVersion.V3);
    ByteBuffer buffer = ByteBuffer.allocate(size);
    original.serialize(buffer, WALFileVersion.V3);
    buffer.flip();

    // Deserialize as V3
    WALMetaData deserialized = WALMetaData.deserialize(buffer, WALFileVersion.V3);

    // Verify basic fields
    assertEquals(10, deserialized.getFirstSearchIndex());
    assertEquals(5, deserialized.getBuffersSize().size());
    assertEquals(Integer.valueOf(100), deserialized.getBuffersSize().get(0));
    assertEquals(Integer.valueOf(200), deserialized.getBuffersSize().get(1));
    assertEquals(Integer.valueOf(150), deserialized.getBuffersSize().get(2));
    assertEquals(Integer.valueOf(300), deserialized.getBuffersSize().get(3));
    assertEquals(Integer.valueOf(250), deserialized.getBuffersSize().get(4));

    // Verify memTable ids
    assertTrue(deserialized.getMemTablesId().contains(1L));
    assertTrue(deserialized.getMemTablesId().contains(2L));

    // Verify V3 epochs
    assertEquals(5, deserialized.getEpochs().size());
    assertEquals(Long.valueOf(1000L), deserialized.getEpochs().get(0));
    assertEquals(Long.valueOf(1000L), deserialized.getEpochs().get(1));
    assertEquals(Long.valueOf(1000L), deserialized.getEpochs().get(2));
    assertEquals(Long.valueOf(2000L), deserialized.getEpochs().get(3));
    assertEquals(Long.valueOf(2000L), deserialized.getEpochs().get(4));

    // Verify V3 syncIndices
    assertEquals(5, deserialized.getSyncIndices().size());
    assertEquals(Long.valueOf(10), deserialized.getSyncIndices().get(0));
    assertEquals(Long.valueOf(11), deserialized.getSyncIndices().get(1));
    assertEquals(Long.valueOf(12), deserialized.getSyncIndices().get(2));
    assertEquals(Long.valueOf(1), deserialized.getSyncIndices().get(3));
    assertEquals(Long.valueOf(2), deserialized.getSyncIndices().get(4));

    // Verify V3 timestamp range
    assertEquals(1600000000000L, deserialized.getMinDataTs());
    assertEquals(1600000001000L, deserialized.getMaxDataTs());
  }

  @Test
  public void testV2DeserializationHasEmptyV3Fields() {
    // Build metadata and serialize as V2 (no epoch/syncIndex)
    WALMetaData original = new WALMetaData();
    original.add(100, 10, 1, 1000L, 10);
    original.add(200, 11, 1, 2000L, 11);

    int size = original.serializedSize(WALFileVersion.V2);
    ByteBuffer buffer = ByteBuffer.allocate(size);
    original.serialize(buffer, WALFileVersion.V2);
    buffer.flip();

    // Deserialize as V2 — should succeed with empty V3 fields
    WALMetaData deserialized = WALMetaData.deserialize(buffer, WALFileVersion.V2);

    assertEquals(10, deserialized.getFirstSearchIndex());
    assertEquals(2, deserialized.getBuffersSize().size());
    // V3 fields should be empty when deserialized as V2
    assertTrue(deserialized.getEpochs().isEmpty());
    assertTrue(deserialized.getSyncIndices().isEmpty());
    assertEquals(Long.MAX_VALUE, deserialized.getMinDataTs());
    assertEquals(Long.MIN_VALUE, deserialized.getMaxDataTs());
  }

  @Test
  public void testV2SerializedSizeSmallerThanV3() {
    WALMetaData meta = new WALMetaData();
    meta.add(100, 10, 1, 1000L, 10);
    meta.add(200, 11, 1, 2000L, 11);
    meta.add(300, 12, 1, 3000L, 12);

    int v2Size = meta.serializedSize(WALFileVersion.V2);
    int v3Size = meta.serializedSize(WALFileVersion.V3);

    // V3 should be larger: 3 entries * 2 longs (epoch + syncIndex) + 2 longs (min/max ts)
    int expectedDiff = 3 * Long.BYTES * 2 + Long.BYTES * 2;
    assertEquals(expectedDiff, v3Size - v2Size);
  }

  @Test
  public void testV3AddAllMerge() {
    WALMetaData meta1 = new WALMetaData();
    meta1.add(100, 10, 1, 1000L, 10);
    meta1.add(200, 11, 1, 1000L, 11);
    meta1.updateTimestampRange(100L);

    WALMetaData meta2 = new WALMetaData();
    meta2.add(300, 12, 2, 2000L, 1);
    meta2.updateTimestampRange(200L);

    meta1.addAll(meta2);

    assertEquals(3, meta1.getBuffersSize().size());
    assertEquals(3, meta1.getEpochs().size());
    assertEquals(3, meta1.getSyncIndices().size());
    assertEquals(Long.valueOf(1000L), meta1.getEpochs().get(0));
    assertEquals(Long.valueOf(1000L), meta1.getEpochs().get(1));
    assertEquals(Long.valueOf(2000L), meta1.getEpochs().get(2));
    assertEquals(Long.valueOf(10), meta1.getSyncIndices().get(0));
    assertEquals(Long.valueOf(11), meta1.getSyncIndices().get(1));
    assertEquals(Long.valueOf(1), meta1.getSyncIndices().get(2));
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
    assertTrue(deserialized.getEpochs().isEmpty());
    assertTrue(deserialized.getSyncIndices().isEmpty());
    assertEquals(Long.MAX_VALUE, deserialized.getMinDataTs());
    assertEquals(Long.MIN_VALUE, deserialized.getMaxDataTs());
  }

  @Test
  public void testV2CompatibleAddDefaultsEpochToZero() {
    // Test the V2-compatible 3-arg add method
    WALMetaData meta = new WALMetaData();
    meta.add(100, 10, 1); // V2-compatible add

    // Should have epoch=0 and syncIndex=searchIndex
    assertEquals(1, meta.getEpochs().size());
    assertEquals(Long.valueOf(0L), meta.getEpochs().get(0));
    assertEquals(Long.valueOf(10L), meta.getSyncIndices().get(0));
  }
}
