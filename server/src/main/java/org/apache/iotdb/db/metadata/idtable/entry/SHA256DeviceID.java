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
package org.apache.iotdb.db.metadata.idtable.entry;

import org.apache.iotdb.db.metadata.idtable.IDTable;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Using sha 256 hash value of device path as device ID */
public class SHA256DeviceID implements IDeviceID {
  // four long value form a 32 bytes sha 256 value
  long l1;
  long l2;
  long l3;
  long l4;

  private static final String SEPARATOR = "_";

  /** using lots of message digest for improving parallelism */
  private static MessageDigest[] md;

  /** number of message digest, for improve parallelism */
  private static final int MD_NUM = 256;

  /** logger */
  private static Logger logger = LoggerFactory.getLogger(IDTable.class);

  static {
    try {
      md = new MessageDigest[MD_NUM];
      for (int i = 0; i < MD_NUM; i++) {
        md[i] = MessageDigest.getInstance("SHA-256");
      }
    } catch (NoSuchAlgorithmException e) {
      logger.error("can't use sha 256 hash on this platform");
    }
  }

  public SHA256DeviceID() {}

  public SHA256DeviceID(String deviceID) {
    // if this device id string is a sha 256 form, we just translate it without sha256
    if (deviceID.indexOf('.') == -1) {
      fromSHA256String(deviceID);
    } else {
      buildSHA256(deviceID);
    }
  }

  /**
   * build device id from a sha 256 string, like "1#1#1#1"
   *
   * @param deviceID a sha 256 string
   */
  private void fromSHA256String(String deviceID) {
    if (deviceID.startsWith("`") && deviceID.endsWith("`")) {
      deviceID = deviceID.substring(1, deviceID.length() - 1);
    }
    String[] part = deviceID.split(SEPARATOR);
    l1 = Long.parseLong(part[0]);
    l2 = Long.parseLong(part[1]);
    l3 = Long.parseLong(part[2]);
    l4 = Long.parseLong(part[3]);
  }

  /**
   * build device id from a device path
   *
   * @param deviceID device path
   */
  private void buildSHA256(String deviceID) {
    byte[] hashVal;
    int slot = calculateSlot(deviceID);

    synchronized (md[slot]) {
      hashVal = md[slot].digest(deviceID.getBytes());
      md[slot].reset();
    }

    l1 = toLong(hashVal, 0);
    l2 = toLong(hashVal, 8);
    l3 = toLong(hashVal, 16);
    l4 = toLong(hashVal, 24);
  }

  /** The probability that each bit of sha 256 is 0 or 1 is equal */
  public int hashCode() {
    return (int) l1;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SHA256DeviceID)) {
      return false;
    }
    SHA256DeviceID that = (SHA256DeviceID) o;
    return l1 == that.l1 && l2 == that.l2 && l3 == that.l3 && l4 == that.l4;
  }

  private long toLong(byte[] array, int start) {
    long res = 0;
    for (int i = 0; i < 8; i++) {
      res <<= 8;
      res |= array[start + i];
    }

    return res;
  }

  /**
   * calculate slot that this deviceID should in
   *
   * @param deviceID device id
   * @return slot number
   */
  private int calculateSlot(String deviceID) {
    int hashVal = deviceID.hashCode();
    return Math.abs(hashVal == Integer.MIN_VALUE ? 0 : hashVal) % MD_NUM;
  }

  @Override
  public String toString() {
    return "SHA256DeviceID{" + "l1=" + l1 + ", l2=" + l2 + ", l3=" + l3 + ", l4=" + l4 + '}';
  }

  @Override
  public String toStringID() {
    return "`" + l1 + SEPARATOR + l2 + SEPARATOR + l3 + SEPARATOR + l4 + "`";
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(l1, byteBuffer);
    ReadWriteIOUtils.write(l2, byteBuffer);
    ReadWriteIOUtils.write(l3, byteBuffer);
    ReadWriteIOUtils.write(l4, byteBuffer);
  }

  public static SHA256DeviceID deserialize(ByteBuffer byteBuffer) {
    SHA256DeviceID sha256DeviceID = new SHA256DeviceID();
    sha256DeviceID.l1 = ReadWriteIOUtils.readLong(byteBuffer);
    sha256DeviceID.l2 = ReadWriteIOUtils.readLong(byteBuffer);
    sha256DeviceID.l3 = ReadWriteIOUtils.readLong(byteBuffer);
    sha256DeviceID.l4 = ReadWriteIOUtils.readLong(byteBuffer);
    return sha256DeviceID;
  }
}
