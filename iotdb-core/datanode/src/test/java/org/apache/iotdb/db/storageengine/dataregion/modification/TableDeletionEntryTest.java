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
package org.apache.iotdb.db.storageengine.dataregion.modification;

import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate.And;
import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate.FullExactMatch;
import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate.NOP;
import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate.SegmentExactMatch;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.read.common.TimeRange;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TableDeletionEntryTest {
  @Test
  public void testSerialization() throws IOException {
    TableDeletionEntry entry =
        new TableDeletionEntry(new DeletionPredicate("table1", new NOP()), new TimeRange(1, 5));
    ByteBuffer buffer = ByteBuffer.allocate(entry.serializedSize());
    entry.serialize(buffer);
    buffer.flip();
    ModEntry deserialized1 = ModEntry.createFrom(buffer);
    assertEquals(entry, deserialized1);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    entry.serialize(bos);
    byte[] byteArray = bos.toByteArray();
    ByteArrayInputStream bis = new ByteArrayInputStream(byteArray);
    ModEntry deserialized2 = ModEntry.createFrom(bis);
    assertEquals(entry, deserialized2);
  }

  @Test
  public void testAffectDevice() {
    TableDeletionEntry entry1 =
        new TableDeletionEntry(new DeletionPredicate("table1", new NOP()), new TimeRange(1, 5));
    TableDeletionEntry entry2 =
        new TableDeletionEntry(
            new DeletionPredicate(
                "table1",
                new FullExactMatch(
                    Factory.DEFAULT_FACTORY.create(new String[] {"table1", "id1", "id2"}))),
            new TimeRange(1, 5));
    TableDeletionEntry entry3 =
        new TableDeletionEntry(
            new DeletionPredicate("table1", new SegmentExactMatch("id1", 1)), new TimeRange(1, 5));
    TableDeletionEntry entry4 =
        new TableDeletionEntry(
            new DeletionPredicate(
                "table1",
                new And(new SegmentExactMatch("id1", 1), new SegmentExactMatch("id2", 2))),
            new TimeRange(1, 5));

    IDeviceID deviceID1 = Factory.DEFAULT_FACTORY.create(new String[] {"table1", "id1", "id2"});
    IDeviceID deviceID2 =
        Factory.DEFAULT_FACTORY.create(new String[] {"table1", "id1", "id2", "id3"});
    IDeviceID deviceID3 = Factory.DEFAULT_FACTORY.create(new String[] {"table2", "id1", "id2"});
    IDeviceID deviceID4 = Factory.DEFAULT_FACTORY.create(new String[] {"table1", "id1"});
    IDeviceID deviceID5 = Factory.DEFAULT_FACTORY.create(new String[] {"table1", null, "id2"});
    IDeviceID deviceID6 = Factory.DEFAULT_FACTORY.create(new String[] {"table1"});

    assertTrue(entry1.affects(deviceID1));
    assertTrue(entry1.affects(deviceID2));
    assertFalse(entry1.affects(deviceID3));
    assertTrue(entry1.affects(deviceID4));
    assertTrue(entry1.affects(deviceID5));
    assertTrue(entry1.affects(deviceID6));

    assertTrue(entry2.affects(deviceID1));
    assertFalse(entry2.affects(deviceID2));
    assertFalse(entry2.affects(deviceID3));
    assertFalse(entry2.affects(deviceID4));
    assertFalse(entry2.affects(deviceID5));
    assertFalse(entry2.affects(deviceID6));

    assertTrue(entry3.affects(deviceID1));
    assertTrue(entry3.affects(deviceID2));
    assertFalse(entry3.affects(deviceID3));
    assertTrue(entry3.affects(deviceID4));
    assertFalse(entry3.affects(deviceID5));
    assertFalse(entry3.affects(deviceID6));

    assertTrue(entry4.affects(deviceID1));
    assertTrue(entry4.affects(deviceID2));
    assertFalse(entry4.affects(deviceID3));
    assertFalse(entry4.affects(deviceID4));
    assertFalse(entry4.affects(deviceID5));
    assertFalse(entry4.affects(deviceID6));
  }

  @Test
  public void testAffectDeviceAndTime() {
    TableDeletionEntry entry1 =
        new TableDeletionEntry(new DeletionPredicate("table1", new NOP()), new TimeRange(1, 5));

    assertTrue(
        entry1.affects(
            Factory.DEFAULT_FACTORY.create(new String[] {"table1", "id1", "id2"}), 0, 3));
    assertTrue(
        entry1.affects(
            Factory.DEFAULT_FACTORY.create(new String[] {"table1", "id1", "id2"}), 2, 6));
    assertTrue(
        entry1.affects(
            Factory.DEFAULT_FACTORY.create(new String[] {"table1", "id1", "id2"}), 0, 1));
    assertTrue(
        entry1.affects(
            Factory.DEFAULT_FACTORY.create(new String[] {"table1", "id1", "id2"}), 5, 8));
    assertTrue(
        entry1.affects(
            Factory.DEFAULT_FACTORY.create(new String[] {"table1", "id1", "id2"}), 2, 4));
    assertTrue(
        entry1.affects(
            Factory.DEFAULT_FACTORY.create(new String[] {"table1", "id1", "id2"}), 1, 5));
    assertTrue(
        entry1.affects(
            Factory.DEFAULT_FACTORY.create(new String[] {"table1", "id1", "id2"}), 0, 15));

    assertFalse(
        entry1.affects(
            Factory.DEFAULT_FACTORY.create(new String[] {"table1", "id1", "id2"}), -1, -3));
    assertFalse(
        entry1.affects(
            Factory.DEFAULT_FACTORY.create(new String[] {"table1", "id1", "id2"}), 11, 13));
  }

  @Test
  public void testAffectMeasurement() {
    TableDeletionEntry entry1 =
        new TableDeletionEntry(
            new DeletionPredicate("table1", new NOP(), Collections.singletonList("s1")),
            new TimeRange(1, 5));
    TableDeletionEntry entry2 =
        new TableDeletionEntry(
            new DeletionPredicate("table1", new NOP(), Collections.emptyList()),
            new TimeRange(1, 5));
    TableDeletionEntry entry3 =
        new TableDeletionEntry(
            new DeletionPredicate("table1", new NOP(), Collections.singletonList("**")),
            new TimeRange(1, 5));
    TableDeletionEntry entry4 =
        new TableDeletionEntry(
            new DeletionPredicate("table1", new NOP(), Collections.singletonList("*")),
            new TimeRange(1, 5));

    assertTrue(entry1.affects("s1"));
    assertFalse(entry1.affects("s2"));

    assertTrue(entry2.affects("s1"));
    assertTrue(entry2.affects("s2"));

    // * and ** are not meaningful in table model deletion
    assertFalse(entry3.affects("s1"));
    assertFalse(entry3.affects("s2"));

    assertFalse(entry4.affects("s1"));
    assertFalse(entry4.affects("s2"));
  }

  @Test
  public void testAffectAll() {
    TableDeletionEntry entry1 =
        new TableDeletionEntry(
            new DeletionPredicate("table1", new NOP(), Collections.singletonList("s1")),
            new TimeRange(1, 5));
    TableDeletionEntry entry2 =
        new TableDeletionEntry(
            new DeletionPredicate("table1", new NOP(), Collections.emptyList()),
            new TimeRange(1, 5));
    TableDeletionEntry entry3 =
        new TableDeletionEntry(new DeletionPredicate("table1", new NOP()), new TimeRange(1, 5));

    assertFalse(
        entry1.affectsAll(Factory.DEFAULT_FACTORY.create(new String[] {"table1", "id1", "id2"})));

    assertTrue(
        entry2.affectsAll(Factory.DEFAULT_FACTORY.create(new String[] {"table1", "id1", "id2"})));

    assertTrue(
        entry3.affectsAll(Factory.DEFAULT_FACTORY.create(new String[] {"table1", "id1", "id2"})));
  }
}
