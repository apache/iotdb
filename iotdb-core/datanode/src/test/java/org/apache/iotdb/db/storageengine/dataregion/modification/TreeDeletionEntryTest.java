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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;

import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TreeDeletionEntryTest {

  @Test
  public void testSerialization() throws IllegalPathException, IOException {
    TreeDeletionEntry entry = new TreeDeletionEntry(new MeasurementPath("root.db1.d1.s1"), 1, 5);
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
  public void testAffectDevice() throws IllegalPathException {
    TreeDeletionEntry entry1 = new TreeDeletionEntry(new MeasurementPath("root.db1.d1.s1"), 1, 5);
    TreeDeletionEntry entry2 = new TreeDeletionEntry(new MeasurementPath("root.db1.d1.**"), 1, 5);
    TreeDeletionEntry entry3 = new TreeDeletionEntry(new MeasurementPath("root.db1.**.s1"), 1, 5);
    TreeDeletionEntry entry4 = new TreeDeletionEntry(new MeasurementPath("root.db1.*.s1"), 1, 5);
    TreeDeletionEntry entry5 = new TreeDeletionEntry(new MeasurementPath("root.db1.d1.*"), 1, 5);

    assertTrue(entry1.affects(Factory.DEFAULT_FACTORY.create("root.db1.d1")));
    assertFalse(entry1.affects(Factory.DEFAULT_FACTORY.create("root.db1.d2")));
    assertFalse(entry1.affects(Factory.DEFAULT_FACTORY.create("root.db1")));
    assertFalse(entry1.affects(Factory.DEFAULT_FACTORY.create("root.db1.d1.d1")));

    assertTrue(entry2.affects(Factory.DEFAULT_FACTORY.create("root.db1.d1")));
    assertFalse(entry2.affects(Factory.DEFAULT_FACTORY.create("root.db1.d2")));
    assertFalse(entry2.affects(Factory.DEFAULT_FACTORY.create("root.db1")));
    assertTrue(entry2.affects(Factory.DEFAULT_FACTORY.create("root.db1.d1.d1")));

    assertTrue(entry3.affects(Factory.DEFAULT_FACTORY.create("root.db1.d1")));
    assertTrue(entry3.affects(Factory.DEFAULT_FACTORY.create("root.db1.d2")));
    assertFalse(entry3.affects(Factory.DEFAULT_FACTORY.create("root.db1")));
    assertTrue(entry3.affects(Factory.DEFAULT_FACTORY.create("root.db1.d1.d1")));

    assertTrue(entry4.affects(Factory.DEFAULT_FACTORY.create("root.db1.d1")));
    assertTrue(entry4.affects(Factory.DEFAULT_FACTORY.create("root.db1.d2")));
    assertFalse(entry4.affects(Factory.DEFAULT_FACTORY.create("root.db1")));
    assertFalse(entry4.affects(Factory.DEFAULT_FACTORY.create("root.db1.d1.d1")));

    assertTrue(entry5.affects(Factory.DEFAULT_FACTORY.create("root.db1.d1")));
    assertFalse(entry5.affects(Factory.DEFAULT_FACTORY.create("root.db1.d2")));
    assertFalse(entry5.affects(Factory.DEFAULT_FACTORY.create("root.db1")));
    assertFalse(entry5.affects(Factory.DEFAULT_FACTORY.create("root.db1.d1.d1")));
  }

  @Test
  public void testAffectDeviceAndTime() throws IllegalPathException {
    TreeDeletionEntry entry1 = new TreeDeletionEntry(new MeasurementPath("root.db1.d1.s1"), 1, 5);
    assertTrue(entry1.affects(Factory.DEFAULT_FACTORY.create("root.db1.d1"), 0, 3));
    assertTrue(entry1.affects(Factory.DEFAULT_FACTORY.create("root.db1.d1"), 2, 6));
    assertTrue(entry1.affects(Factory.DEFAULT_FACTORY.create("root.db1.d1"), 0, 1));
    assertTrue(entry1.affects(Factory.DEFAULT_FACTORY.create("root.db1.d1"), 5, 8));
    assertTrue(entry1.affects(Factory.DEFAULT_FACTORY.create("root.db1.d1"), 2, 4));
    assertTrue(entry1.affects(Factory.DEFAULT_FACTORY.create("root.db1.d1"), 1, 5));
    assertTrue(entry1.affects(Factory.DEFAULT_FACTORY.create("root.db1.d1"), 0, 15));

    assertFalse(entry1.affects(Factory.DEFAULT_FACTORY.create("root.db1.d1"), -1, -3));
    assertFalse(entry1.affects(Factory.DEFAULT_FACTORY.create("root.db1.d1"), 11, 13));
  }

  @Test
  public void testAffectMeasurement() throws IllegalPathException {
    TreeDeletionEntry entry1 = new TreeDeletionEntry(new MeasurementPath("root.db1.d1.s1"), 1, 5);
    TreeDeletionEntry entry2 = new TreeDeletionEntry(new MeasurementPath("root.db1.d1.**"), 1, 5);
    TreeDeletionEntry entry3 = new TreeDeletionEntry(new MeasurementPath("root.db1.d1.*"), 1, 5);

    assertTrue(entry1.affects("s1"));
    assertFalse(entry1.affects("s2"));

    assertTrue(entry2.affects("s1"));
    assertTrue(entry2.affects("s2"));

    assertTrue(entry3.affects("s1"));
    assertTrue(entry3.affects("s2"));
  }

  @Test
  public void testAffectAll() throws IllegalPathException {
    TreeDeletionEntry entry1 = new TreeDeletionEntry(new MeasurementPath("root.db1.d1.s1"), 1, 5);
    TreeDeletionEntry entry2 = new TreeDeletionEntry(new MeasurementPath("root.db1.d1.**"), 1, 5);
    TreeDeletionEntry entry3 = new TreeDeletionEntry(new MeasurementPath("root.db1.**.s1"), 1, 5);
    TreeDeletionEntry entry4 = new TreeDeletionEntry(new MeasurementPath("root.db1.*.s1"), 1, 5);
    TreeDeletionEntry entry5 = new TreeDeletionEntry(new MeasurementPath("root.db1.d1.*"), 1, 5);

    assertFalse(entry1.affectsAll(Factory.DEFAULT_FACTORY.create("root.db1.d1")));
    assertFalse(entry1.affectsAll(Factory.DEFAULT_FACTORY.create("root.db1.d2")));
    assertFalse(entry1.affectsAll(Factory.DEFAULT_FACTORY.create("root.db1")));
    assertFalse(entry1.affectsAll(Factory.DEFAULT_FACTORY.create("root.db1.d1.d1")));

    assertTrue(entry2.affectsAll(Factory.DEFAULT_FACTORY.create("root.db1.d1")));
    assertFalse(entry2.affectsAll(Factory.DEFAULT_FACTORY.create("root.db1.d2")));
    assertFalse(entry2.affectsAll(Factory.DEFAULT_FACTORY.create("root.db1")));
    assertTrue(entry2.affectsAll(Factory.DEFAULT_FACTORY.create("root.db1.d1.d1")));

    assertFalse(entry3.affectsAll(Factory.DEFAULT_FACTORY.create("root.db1.d1")));
    assertFalse(entry3.affectsAll(Factory.DEFAULT_FACTORY.create("root.db1.d2")));
    assertFalse(entry3.affectsAll(Factory.DEFAULT_FACTORY.create("root.db1")));
    assertFalse(entry3.affectsAll(Factory.DEFAULT_FACTORY.create("root.db1.d1.d1")));

    assertFalse(entry4.affectsAll(Factory.DEFAULT_FACTORY.create("root.db1.d1")));
    assertFalse(entry4.affectsAll(Factory.DEFAULT_FACTORY.create("root.db1.d2")));
    assertFalse(entry4.affectsAll(Factory.DEFAULT_FACTORY.create("root.db1")));
    assertFalse(entry4.affectsAll(Factory.DEFAULT_FACTORY.create("root.db1.d1.d1")));

    assertTrue(entry5.affectsAll(Factory.DEFAULT_FACTORY.create("root.db1.d1")));
    assertFalse(entry5.affectsAll(Factory.DEFAULT_FACTORY.create("root.db1.d2")));
    assertFalse(entry5.affectsAll(Factory.DEFAULT_FACTORY.create("root.db1")));
    assertFalse(entry5.affectsAll(Factory.DEFAULT_FACTORY.create("root.db1.d1.d1")));
  }
}
