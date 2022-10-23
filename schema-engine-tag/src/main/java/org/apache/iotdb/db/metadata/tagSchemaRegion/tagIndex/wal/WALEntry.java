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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.wal;

import org.apache.iotdb.lsm.wal.IWALRecord;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** represents a record in the wal file */
public class WALEntry implements IWALRecord<String, Integer> {

  // can be insertion(1) or deletion(2)
  private int type;

  // keys at each level
  private List<String> keys;

  // device id
  private int deviceID;

  public WALEntry() {
    super();
  }

  public WALEntry(int type, List<String> keys, int deviceID) {
    super();
    this.type = type;
    this.keys = keys;
    this.deviceID = deviceID;
  }

  /**
   * serialize the wal entry
   *
   * @param buffer byte buffer
   */
  @Override
  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(type, buffer);
    ReadWriteIOUtils.write(deviceID, buffer);
    ReadWriteIOUtils.write(keys.size(), buffer);
    for (String key : keys) {
      ReadWriteIOUtils.write(key, buffer);
    }
  }

  /**
   * deserialize from DataInputStream
   *
   * @param stream data input stream
   * @throws IOException
   */
  @Override
  public void deserialize(DataInputStream stream) throws IOException {
    this.type = stream.readInt();
    this.deviceID = stream.readInt();
    int length = stream.readInt();
    this.keys = new ArrayList<>();
    for (int i = 0; i < length; i++) {
      String key = ReadWriteIOUtils.readString(stream);
      keys.add(key);
    }
  }

  /**
   * generate wal record using prototyping pattern
   *
   * @return wal record
   */
  @Override
  public IWALRecord clone() {
    try {
      return (IWALRecord) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new AssertionError(e.getMessage());
    }
  }

  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
  }

  @Override
  public List<String> getKeys() {
    return keys;
  }

  @Override
  public Integer getValue() {
    return getDeviceID();
  }

  public void setKeys(List<String> keys) {
    this.keys = keys;
  }

  public int getDeviceID() {
    return deviceID;
  }

  public void setDeviceID(int deviceID) {
    this.deviceID = deviceID;
  }

  @Override
  public String toString() {
    return "WALEntry{" + "type=" + type + ", keys=" + keys + ", deviceID=" + deviceID + '}';
  }
}
