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

package org.apache.tsfile.file.metadata;

import org.apache.tsfile.exception.IllegalDeviceIDException;
import org.apache.tsfile.file.metadata.IDeviceID.Deserializer;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class IDeviceIDTest {

  @Test
  public void testStartWith() {
    IDeviceID deviceID = Factory.DEFAULT_FACTORY.create("root.a.b.c.d");
    assertTrue(deviceID.startWith("root.a"));
    assertTrue(deviceID.startWith("root.a."));
    assertTrue(deviceID.startWith("root.a.b"));
    assertTrue(deviceID.startWith("root.a.b."));
    assertTrue(deviceID.startWith("root.a.b.c"));
    assertTrue(deviceID.startWith("root.a.b.c.d"));

    assertFalse(deviceID.startWith("root.b"));
    assertFalse(deviceID.startWith("root.a.b.d"));
    assertFalse(deviceID.startWith("root.a.b.c.e"));

    assertFalse(deviceID.startWith("root.a.bb"));
    assertFalse(deviceID.startWith("root.a.b.cc"));
    assertFalse(deviceID.startWith("root.a.b.c.dd"));

    assertFalse(deviceID.startWith("root.a.b.c.."));
    assertFalse(deviceID.startWith("root.a.b.c.d."));
    assertFalse(deviceID.startWith("root.a.b.c.d.e"));
    assertFalse(deviceID.startWith("root.a..b.c"));

    deviceID = Factory.DEFAULT_FACTORY.create("root.aaaa.b.c.d");
    assertTrue(deviceID.startWith("root.a"));
  }

  @Test
  public void testIsTableModel() {
    IDeviceID deviceID = Factory.DEFAULT_FACTORY.create("root.a.b.c.d");
    assertFalse(deviceID.isTableModel());
    deviceID = Factory.DEFAULT_FACTORY.create("root.b.c");
    assertFalse(deviceID.isTableModel());
    deviceID = Factory.DEFAULT_FACTORY.create("roota.b.c.d");
    assertTrue(deviceID.isTableModel());
  }

  @Test
  public void testMatchDatabaseName() {
    IDeviceID deviceID = Factory.DEFAULT_FACTORY.create("root.a.b.c.d");
    assertTrue(deviceID.matchDatabaseName("root.a"));
    assertFalse(deviceID.matchDatabaseName("root.a."));
    assertTrue(deviceID.matchDatabaseName("root.a.b"));
    assertFalse(deviceID.matchDatabaseName("root.a.b."));
    assertTrue(deviceID.matchDatabaseName("root.a.b.c"));
    assertTrue(deviceID.matchDatabaseName("root.a.b.c.d"));

    assertFalse(deviceID.matchDatabaseName("root.b"));
    assertFalse(deviceID.matchDatabaseName("root.a.b.d"));
    assertFalse(deviceID.matchDatabaseName("root.a.b.c.e"));

    assertFalse(deviceID.matchDatabaseName("root.a.bb"));
    assertFalse(deviceID.matchDatabaseName("root.a.b.cc"));
    assertFalse(deviceID.matchDatabaseName("root.a.b.c.dd"));

    assertFalse(deviceID.matchDatabaseName("root.a.b.c.."));
    assertFalse(deviceID.matchDatabaseName("root.a.b.c.d."));
    assertFalse(deviceID.matchDatabaseName("root.a.b.c.d.e"));
    assertFalse(deviceID.matchDatabaseName("root.a..b.c"));

    deviceID = Factory.DEFAULT_FACTORY.create("root.aaaa.b.c.d");
    assertFalse(deviceID.matchDatabaseName("root.a"));
  }

  @Test
  public void testWithNull() {
    // auto adding tailing null for one segment id
    IDeviceID deviceID = Factory.DEFAULT_FACTORY.create(new String[] {"table1"});
    assertEquals(1, deviceID.segmentNum());
    assertEquals("table1", deviceID.segment(0));

    // removing tailing null
    deviceID = Factory.DEFAULT_FACTORY.create(new String[] {"table1", "a", null, null});
    assertEquals(2, deviceID.segmentNum());
    assertEquals("table1", deviceID.segment(0));
    assertEquals("a", deviceID.segment(1));

    // removing tailing null ant not leaving the last null
    deviceID = Factory.DEFAULT_FACTORY.create(new String[] {"table1", null, null, null});
    assertEquals(1, deviceID.segmentNum());
    assertEquals("table1", deviceID.segment(0));

    // all null
    assertThrows(
        IllegalDeviceIDException.class,
        () -> Factory.DEFAULT_FACTORY.create(new String[] {null, null, null, null}));
    assertThrows(
        IllegalDeviceIDException.class, () -> Factory.DEFAULT_FACTORY.create(new String[] {}));
  }

  @Test
  public void testSerialize() throws IOException {
    testSerialize(Factory.DEFAULT_FACTORY.create("root"));
    testSerialize(Factory.DEFAULT_FACTORY.create("root.a"));
    testSerialize(Factory.DEFAULT_FACTORY.create("root.a.b"));
    testSerialize(Factory.DEFAULT_FACTORY.create("root.a.b.c"));
    testSerialize(Factory.DEFAULT_FACTORY.create("root.a.b.c.d"));
    testSerialize(Factory.DEFAULT_FACTORY.create(new String[] {"root", "a", null, "c", "d"}));
  }

  private void testSerialize(IDeviceID deviceID) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(deviceID.serializedSize());
    deviceID.serialize(buffer);
    buffer.flip();
    IDeviceID deserialized = Deserializer.DEFAULT_DESERIALIZER.deserializeFrom(buffer);
    assertEquals(deserialized, deviceID);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    deviceID.serialize(byteArrayOutputStream);
    assertEquals(deviceID.serializedSize(), byteArrayOutputStream.size());
    buffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    deserialized = Deserializer.DEFAULT_DESERIALIZER.deserializeFrom(buffer);
    assertEquals(deserialized, deviceID);
  }
}
