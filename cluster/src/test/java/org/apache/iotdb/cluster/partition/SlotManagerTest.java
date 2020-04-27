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
package org.apache.iotdb.cluster.partition;

import static org.apache.iotdb.cluster.partition.SlotManager.SlotStatus.NULL;
import static org.apache.iotdb.cluster.partition.SlotManager.SlotStatus.PULLING;
import static org.apache.iotdb.cluster.partition.SlotManager.SlotStatus.PULLING_WRITABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.junit.Before;
import org.junit.Test;

public class SlotManagerTest {

  private SlotManager slotManager;

  @Before
  public void setUp() {
    int testSlotNum = 100;
    slotManager = new SlotManager(testSlotNum);
  }

  @Test
  public void waitSlot() {
    slotManager.waitSlot(0);
    slotManager.setToPulling(0, null);
    new Thread(() -> {
      try {
        Thread.sleep(200);
        slotManager.setToNull(0);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }).start();
    slotManager.waitSlot(0);
  }

  @Test
  public void waitSlotForWrite() {
    slotManager.waitSlot(0);
    slotManager.setToPullingWritable(0);
    slotManager.waitSlotForWrite(0);
    slotManager.setToPulling(0, null);
    new Thread(() -> {
      try {
        Thread.sleep(200);
        slotManager.setToNull(0);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }).start();
    slotManager.waitSlotForWrite(0);
  }

  @Test
  public void getStatus() {
    assertEquals(NULL, slotManager.getStatus(0));
    slotManager.setToPullingWritable(0);
    assertEquals(PULLING_WRITABLE, slotManager.getStatus(0));
    slotManager.setToPulling(0, null);
    assertEquals(PULLING, slotManager.getStatus(0));
    slotManager.setToNull(0);
    assertEquals(NULL, slotManager.getStatus(0));
  }

  @Test
  public void getSource() {
    assertNull(slotManager.getSource(0));
    Node source = new Node();
    slotManager.setToPulling(0, source);
    assertEquals(source, slotManager.getSource(0));
    slotManager.setToPullingWritable(0);
    assertEquals(source, slotManager.getSource(0));
    slotManager.setToNull(0);
    assertNull(slotManager.getSource(0));
  }
}
