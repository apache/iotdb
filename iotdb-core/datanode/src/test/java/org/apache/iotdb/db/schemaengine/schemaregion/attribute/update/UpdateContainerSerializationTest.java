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

package org.apache.iotdb.db.schemaengine.schemaregion.attribute.update;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UpdateContainerSerializationTest {

  @Test
  public void testTableAddedDuringSerialization() throws Exception {
    assertConcurrentUpdateRoundTrip(0);
  }

  @Test
  public void testDeviceAddedDuringSerialization() throws Exception {
    assertConcurrentUpdateRoundTrip(1);
  }

  @Test
  public void testAttributeAddedDuringSerialization() throws Exception {
    assertConcurrentUpdateRoundTrip(2);
  }

  private void assertConcurrentUpdateRoundTrip(final int level) throws Exception {
    final UpdateDetailContainer container = new UpdateDetailContainer();
    final AtomicReference<Runnable> concurrentUpdate = new AtomicReference<>(() -> {});
    final ConcurrentMap<String, Binary> attributes =
        new ConcurrentHashMap<String, Binary>() {
          @Override
          public Set<Map.Entry<String, Binary>> entrySet() {
            // A weakly consistent iterator may miss an insertion made after traversal starts.
            // Apply it here so the race is reproducible without relying on thread scheduling.
            final Set<Map.Entry<String, Binary>> entries = new HashSet<>(super.entrySet());
            concurrentUpdate.getAndSet(() -> {}).run();
            return entries;
          }
        };
    final Binary value = new Binary("value", TSFileConfig.STRING_CHARSET);
    attributes.put("attribute", value);
    final ConcurrentMap<List<String>, ConcurrentMap<String, Binary>> devices =
        new ConcurrentHashMap<>();
    devices.put(Collections.singletonList("device"), attributes);
    container.getUpdateMap().put("table", devices);
    concurrentUpdate.set(
        () -> {
          if (level == 0) {
            container.getUpdateMap().put("new_table", new ConcurrentHashMap<>());
          } else if (level == 1) {
            devices.put(Collections.singletonList("new_device"), new ConcurrentHashMap<>());
          } else {
            attributes.put("new_attribute", value);
          }
        });

    final AtomicBoolean hasRemaining = new AtomicBoolean();
    final byte[] bytes =
        container.getUpdateContent(new AtomicInteger(Integer.MAX_VALUE), hasRemaining);
    assertFalse(hasRemaining.get());
    // Check the lengths before decoding to avoid allocating from a corrupt string length.
    final ByteArrayInputStream input = new ByteArrayInputStream(bytes);
    assertTrue(ReadWriteIOUtils.readBool(input));
    assertEquals(1, ReadWriteIOUtils.readInt(input));
    assertEquals("table", ReadWriteIOUtils.readString(input));
    assertEquals(1, ReadWriteIOUtils.readInt(input));
    assertEquals(1, ReadWriteIOUtils.readInt(input));
    assertEquals("device", ReadWriteIOUtils.readString(input));
    assertEquals(1, ReadWriteIOUtils.readInt(input));

    final UpdateDetailContainer decoded = new UpdateDetailContainer();
    final ByteArrayInputStream roundTrip = new ByteArrayInputStream(bytes);
    assertTrue(ReadWriteIOUtils.readBool(roundTrip));
    decoded.deserialize(roundTrip);
    assertEquals(0, roundTrip.available());
    assertEquals(
        Collections.singletonMap(
            "table",
            Collections.singletonMap(
                Collections.singletonList("device"), Collections.singletonMap("attribute", value))),
        decoded.getUpdateMap());
  }
}
