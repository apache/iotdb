/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.cluster.partition;

import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SlotManager manages the status of the slots involved during a data transfer (data slot
 * ownership changes caused by node removals or additions) of a data group member.
 */
public class SlotManager {

  private static final Logger logger = LoggerFactory.getLogger(SlotManager.class);
  private static final long SLOT_WAIT_INTERVAL_MS = 10;

  /**
   * the serial number of a slot -> the status and source of a slot
   */
  private Map<Long, SlotDescriptor> idSlotMap;

  public SlotManager(long totalSlotNumber) {
    idSlotMap = new HashMap<>();
    for (long i = 0; i < totalSlotNumber; i++) {
      idSlotMap.put(i, new SlotDescriptor(SlotStatus.NULL));
    }
  }

  /**
   * Wait until the status of the slot become NULL
   * @param slotId
   */
  public void waitSlot(long slotId) {
    SlotDescriptor slotDescriptor = idSlotMap.get(slotId);
    while (true) {
      synchronized (slotDescriptor) {
        if (slotDescriptor.slotStatus != SlotStatus.NULL) {
          try {
            slotDescriptor.wait(SLOT_WAIT_INTERVAL_MS);
          } catch (InterruptedException e) {
            logger.error("Unexpected interruption when waiting for slot {}", slotId, e);
          }
        } else {
          return;
        }
      }
    }
  }

  /**
   * Wait until the status of the slot become NULL of PULLING_WRITABLE
   * @param slotId
   */
  public void waitSlotForWrite(long slotId) {
    SlotDescriptor slotDescriptor = idSlotMap.get(slotId);
    while (true) {
      synchronized (slotDescriptor) {
        if (slotDescriptor.slotStatus != SlotStatus.NULL &&
            slotDescriptor.slotStatus != SlotStatus.PULLING_WRITABLE) {
          try {
            slotDescriptor.wait(SLOT_WAIT_INTERVAL_MS);
          } catch (InterruptedException e) {
            logger.error("Unexpected interruption when waiting for slot {}", slotId, e);
          }
        } else {
          return;
        }
      }
    }
  }

  /**
   *
   * @param slotId
   * @return the SlotStatus of a slot
   */
  public SlotStatus getStatus(long slotId) {
    return idSlotMap.get(slotId).slotStatus;
  }

  /**
   *
   * @param slotId
   * @return the source of a slot if it is being pulled, or null if it is not being pulled
   */
  public Node getSource(long slotId) {
    return idSlotMap.get(slotId).source;
  }

  /**
   * Set the status of slot "slotId" to PULLING and its source to "source".
   * @param slotId
   * @param source
   */
  public void setToPulling(long slotId, Node source) {
    SlotDescriptor slotDescriptor = idSlotMap.get(slotId);
    synchronized (slotDescriptor) {
      slotDescriptor.slotStatus = SlotStatus.PULLING;
      slotDescriptor.source = source;
    }
  }

  /**
   * Set the status of slot "slotId" to PULLING_WRITABLE.
   * @param slotId
   */
  public void setToPullingWritable(long slotId) {
    SlotDescriptor slotDescriptor = idSlotMap.get(slotId);
    synchronized (slotDescriptor) {
      slotDescriptor.slotStatus = SlotStatus.PULLING_WRITABLE;
      slotDescriptor.notifyAll();
    }
  }

  /**
   * Set the status of slot "slotId" to NULL.
   * @param slotId
   */
  public void setToNull(long slotId) {
    SlotDescriptor slotDescriptor = idSlotMap.get(slotId);
    synchronized (slotDescriptor) {
      slotDescriptor.slotStatus = SlotStatus.NULL;
      slotDescriptor.source = null;
      slotDescriptor.notifyAll();
    }
  }


  enum SlotStatus {
    // the slot has pulled data or does not belong to this member
    NULL,
    // the slot is pulling data and writes into it should be blocked and reads of it should merge
    // the source
    PULLING,
    // the slot is pulling data and reads of it should merge the source
    PULLING_WRITABLE
  }

  private static class SlotDescriptor {
    private SlotStatus slotStatus;
    private Node source;

    public SlotDescriptor(SlotStatus slotStatus) {
      this.slotStatus = slotStatus;
    }
  }
}
