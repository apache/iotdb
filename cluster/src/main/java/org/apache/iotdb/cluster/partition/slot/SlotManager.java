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

package org.apache.iotdb.cluster.partition.slot;

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.NodeSerializeUtils;
import org.apache.iotdb.db.exception.StorageEngineException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SlotManager manages the status of the slots involved during a data transfer (data slot ownership
 * changes caused by node removals or additions) of a data group member.
 */
public class SlotManager {

  private static final Logger logger = LoggerFactory.getLogger(SlotManager.class);
  private static final long SLOT_WAIT_INTERVAL_MS = 10;
  private static final long SLOT_WAIT_THRESHOLD_MS = 2000;
  private static final String SLOT_FILE_NAME = "SLOT_STATUS";

  private String slotFilePath;

  /** the serial number of a slot -> the status and source of a slot */
  private Map<Integer, SlotDescriptor> idSlotMap;

  private String memberName;

  public SlotManager(long totalSlotNumber, String memberDir, String memberName) {
    if (memberDir != null) {
      this.slotFilePath = memberDir + File.separator + SLOT_FILE_NAME;
    }
    this.memberName = memberName;
    if (!load()) {
      init(totalSlotNumber);
    }
  }

  private void init(long totalSlotNumber) {
    idSlotMap = new ConcurrentHashMap<>();
    for (int i = 0; i < totalSlotNumber; i++) {
      idSlotMap.put(i, new SlotDescriptor(SlotStatus.NULL));
    }
  }

  /**
   * Wait until the status of the slot becomes NULL
   *
   * @param slotId
   */
  public void waitSlot(int slotId) {
    SlotDescriptor slotDescriptor = idSlotMap.get(slotId);
    long startTime = System.currentTimeMillis();
    while (true) {
      synchronized (slotDescriptor) {
        if (slotDescriptor.slotStatus == SlotStatus.PULLING
            || slotDescriptor.slotStatus == SlotStatus.PULLING_WRITABLE) {
          try {
            slotDescriptor.wait(SLOT_WAIT_INTERVAL_MS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Unexpected interruption when waiting for slot {}", slotId, e);
          }
        } else {
          long cost = System.currentTimeMillis() - startTime;
          if (cost > SLOT_WAIT_THRESHOLD_MS) {
            logger.info("Wait slot {} cost {}ms", slotId, cost);
          }
          return;
        }
      }
    }
  }

  /**
   * Wait until the status of the slot becomes NULL or PULLING_WRITABLE
   *
   * @param slotId
   */
  public void waitSlotForWrite(int slotId) throws StorageEngineException {
    SlotDescriptor slotDescriptor = idSlotMap.get(slotId);
    long startTime = System.currentTimeMillis();
    while (true) {
      synchronized (slotDescriptor) {
        if (slotDescriptor.slotStatus == SlotStatus.PULLING) {
          try {
            if ((System.currentTimeMillis() - startTime) >= SLOT_WAIT_THRESHOLD_MS) {
              throw new StorageEngineException(
                  String.format("The status of slot %d is still PULLING after 5s.", slotId));
            }
            slotDescriptor.wait(SLOT_WAIT_INTERVAL_MS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Unexpected interruption when waiting for slot {}", slotId, e);
          }
        } else {
          return;
        }
      }
    }
  }

  /** If a slot in the status of PULLING or PULLING_WRITABLE, reads of it should merge the source */
  public boolean checkSlotInDataMigrationStatus(int slotId) {
    SlotDescriptor slotDescriptor = idSlotMap.get(slotId);
    return slotDescriptor.slotStatus == SlotStatus.PULLING
        || slotDescriptor.slotStatus == SlotStatus.PULLING_WRITABLE;
  }

  public boolean checkSlotInMetaMigrationStatus(int slotId) {
    SlotDescriptor slotDescriptor = idSlotMap.get(slotId);
    return slotDescriptor.slotStatus == SlotStatus.PULLING;
  }

  /**
   * @param slotId
   * @return the SlotStatus of a slot
   */
  public SlotStatus getStatus(int slotId) {
    return idSlotMap.get(slotId).slotStatus;
  }

  /**
   * @param slotId
   * @return the source of a slot if it is being pulled, or null if it is not being pulled
   */
  public Node getSource(int slotId) {
    return idSlotMap.get(slotId).source;
  }

  /**
   * Set the status of slot "slotId" to PULLING and its source to "source".
   *
   * @param slotId
   * @param source
   */
  public void setToPulling(int slotId, Node source) {
    setToPulling(slotId, source, true);
  }

  public void setToPulling(int slotId, Node source, boolean needSave) {
    SlotDescriptor slotDescriptor = idSlotMap.get(slotId);
    synchronized (slotDescriptor) {
      slotDescriptor.slotStatus = SlotStatus.PULLING;
      slotDescriptor.source = source;
    }
    if (needSave) {
      save();
    }
  }

  /**
   * Set the status of slot "slotId" to PULLING_WRITABLE.
   *
   * @param slotId
   */
  public void setToPullingWritable(int slotId) {
    setToPullingWritable(slotId, true);
  }

  public void setToPullingWritable(int slotId, boolean needSave) {
    SlotDescriptor slotDescriptor = idSlotMap.get(slotId);
    synchronized (slotDescriptor) {
      slotDescriptor.slotStatus = SlotStatus.PULLING_WRITABLE;
      slotDescriptor.notifyAll();
    }
    if (needSave) {
      save();
    }
  }

  /**
   * Set the status of slot "slotId" to NULL.
   *
   * @param slotId
   */
  public void setToNull(int slotId) {
    setToNull(slotId, true);
  }

  public void setToNull(int slotId, boolean needSave) {
    SlotDescriptor slotDescriptor = idSlotMap.get(slotId);
    synchronized (slotDescriptor) {
      slotDescriptor.slotStatus = SlotStatus.NULL;
      slotDescriptor.source = null;
      slotDescriptor.notifyAll();
    }
    if (needSave) {
      save();
    }
  }

  public void setToSending(int slotId) {
    setToSending(slotId, true);
  }

  public void setToSending(int slotId, boolean needSave) {
    // only NULL slots can be set to SENDING
    waitSlot(slotId);
    SlotDescriptor slotDescriptor = idSlotMap.get(slotId);
    synchronized (slotDescriptor) {
      slotDescriptor.slotStatus = SlotStatus.SENDING;
      slotDescriptor.snapshotReceivedCount = 0;
    }
    if (needSave) {
      save();
    }
  }

  private void setToSent(int slotId) {
    SlotDescriptor slotDescriptor = idSlotMap.get(slotId);
    synchronized (slotDescriptor) {
      slotDescriptor.slotStatus = SlotStatus.SENT;
    }
  }

  /**
   * If a slot is in LOSING status and one member of the remote group has pulled snapshot, the
   * method should be called so eventually we can clear data of the slot.
   *
   * @param slotId
   * @return how many members in the remote group has received the snapshot (including this
   *     invocation).
   */
  public int sentOneReplication(int slotId) {
    return sentOneReplication(slotId, true);
  }

  public int sentOneReplication(int slotId, boolean needSave) {
    SlotDescriptor slotDescriptor = idSlotMap.get(slotId);
    synchronized (slotDescriptor) {
      int sentReplicaNum = ++slotDescriptor.snapshotReceivedCount;
      if (sentReplicaNum >= ClusterDescriptor.getInstance().getConfig().getReplicationNum()) {
        setToSent(slotId);
      }
      if (needSave) {
        save();
      }
      return sentReplicaNum;
    }
  }

  private boolean load() {
    if (slotFilePath == null) {
      return false;
    }
    File slotFile = new File(slotFilePath);
    if (!slotFile.exists()) {
      return false;
    }

    try (FileInputStream fileInputStream = new FileInputStream(slotFile);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)) {
      byte[] bytes = new byte[(int) slotFile.length()];
      int read = bufferedInputStream.read(bytes);
      if (read != slotFile.length() && logger.isWarnEnabled()) {
        logger.warn(
            "SlotManager in {} read size does not equal to file size: {}/{}",
            slotFilePath,
            read,
            slotFile.length());
      }
      deserialize(ByteBuffer.wrap(bytes));
      return true;
    } catch (Exception e) {
      logger.warn("Cannot deserialize slotManager from {}", slotFilePath, e);
      return false;
    }
  }

  public synchronized void save() {
    if (slotFilePath == null) {
      return;
    }
    File slotFile = new File(slotFilePath);
    if (!slotFile.getParentFile().exists() && !slotFile.getParentFile().mkdirs()) {
      logger.warn("Cannot mkdirs for {}", slotFile);
    }
    try (FileOutputStream outputStream = new FileOutputStream(slotFilePath);
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);
        DataOutputStream dataOutputStream = new DataOutputStream(bufferedOutputStream)) {
      serialize(dataOutputStream);
    } catch (IOException e) {
      logger.warn("SlotManager in {} cannot be saved", slotFilePath, e);
    }
  }

  public int getSloNumInDataMigration() {
    int res = 0;
    for (Entry<Integer, SlotDescriptor> entry : idSlotMap.entrySet()) {
      SlotDescriptor descriptor = entry.getValue();
      if (descriptor.slotStatus == SlotStatus.PULLING
          || descriptor.slotStatus == SlotStatus.PULLING_WRITABLE) {
        logger.info(
            "{}: slot {} is in data migration, status is {}",
            memberName,
            entry.getKey(),
            descriptor.slotStatus);
        res++;
      }
    }
    return res;
  }

  private void serialize(DataOutputStream outputStream) throws IOException {
    outputStream.writeInt(idSlotMap.size());
    for (Entry<Integer, SlotDescriptor> integerSlotDescriptorEntry : idSlotMap.entrySet()) {
      outputStream.writeInt(integerSlotDescriptorEntry.getKey());
      integerSlotDescriptorEntry.getValue().serialize(outputStream);
    }
  }

  private void deserialize(ByteBuffer buffer) {
    int slotNum = buffer.getInt();
    idSlotMap = new ConcurrentHashMap<>(slotNum);
    for (int i = 0; i < slotNum; i++) {
      int slotId = buffer.getInt();
      SlotDescriptor descriptor = SlotDescriptor.deserialize(buffer);
      idSlotMap.put(slotId, descriptor);
    }
  }

  public enum SlotStatus {
    // the slot has pulled data or does not belong to this member
    NULL,
    // the slot is pulling data and writes into it should be blocked and reads of it should merge
    // the source
    PULLING,
    // the slot is pulling data and reads of it should merge the source
    PULLING_WRITABLE,
    // the slot is allocated to another group but that group has not pulled data
    SENDING,
    // the new owner of the slot has pulled data, and the local data can be removed
    SENT
  }

  private static class SlotDescriptor {
    private volatile SlotStatus slotStatus;
    private Node source;
    // in LOSING status, how many members in the new owner have pulled data
    private volatile int snapshotReceivedCount;

    SlotDescriptor() {}

    SlotDescriptor(SlotStatus slotStatus) {
      this.slotStatus = slotStatus;
    }

    private void serialize(DataOutputStream outputStream) throws IOException {
      outputStream.writeInt(slotStatus.ordinal());
      if (slotStatus == SlotStatus.PULLING || slotStatus == SlotStatus.PULLING_WRITABLE) {
        NodeSerializeUtils.serialize(source, outputStream);
      } else if (slotStatus == SlotStatus.SENDING) {
        outputStream.writeInt(snapshotReceivedCount);
      }
    }

    private static SlotDescriptor deserialize(ByteBuffer buffer) {
      SlotDescriptor descriptor = new SlotDescriptor();
      descriptor.slotStatus = SlotStatus.values()[buffer.getInt()];
      if (descriptor.slotStatus == SlotStatus.PULLING
          || descriptor.slotStatus == SlotStatus.PULLING_WRITABLE) {
        descriptor.source = new Node();
        NodeSerializeUtils.deserialize(descriptor.source, buffer);
      } else if (descriptor.slotStatus == SlotStatus.SENDING) {
        descriptor.snapshotReceivedCount = buffer.getInt();
      }
      return descriptor;
    }
  }
}
