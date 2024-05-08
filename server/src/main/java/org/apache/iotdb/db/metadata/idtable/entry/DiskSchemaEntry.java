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

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * the disk schema entry of schema entry of id table. This is a po class, so every field is public
 */
public class DiskSchemaEntry {

  public static final String TOMBSTONE = "tombstone_record";
  // id form device path, eg: 1#2#3#4
  public String deviceID;

  // full timeseries path, eg: root.sg1.d1.s1
  public String seriesKey;

  // measurement name of timeseries: eg: s1
  public String measurementName;

  // timeseries data type
  public byte type;

  // timeseries encoding type
  public byte encoding;

  // timeseries compressor type
  public byte compressor;

  // whether this device is aligned
  public boolean isAligned;

  // this entry's serialized size
  public transient long entrySize;

  private DiskSchemaEntry() {}

  public DiskSchemaEntry(
      String deviceID,
      String seriesKey,
      String measurementName,
      byte type,
      byte encoding,
      byte compressor,
      boolean isAligned) {
    this.deviceID = deviceID;
    this.seriesKey = seriesKey;
    this.measurementName = measurementName;
    this.type = type;
    this.encoding = encoding;
    this.compressor = compressor;
    this.isAligned = isAligned;
  }

  /**
   * get device path
   *
   * @return device path
   */
  public String getDevicePath() {
    return seriesKey.substring(0, seriesKey.length() - measurementName.length() - 1);
  }

  public int serialize(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(deviceID, outputStream);
    byteLen += ReadWriteIOUtils.write(seriesKey, outputStream);
    byteLen += ReadWriteIOUtils.write(measurementName, outputStream);
    byteLen += ReadWriteIOUtils.write(type, outputStream);
    byteLen += ReadWriteIOUtils.write(encoding, outputStream);
    byteLen += ReadWriteIOUtils.write(compressor, outputStream);
    byteLen += ReadWriteIOUtils.write(isAligned, outputStream);

    byteLen += ReadWriteIOUtils.write(byteLen, outputStream);
    entrySize = byteLen;

    return byteLen;
  }

  public static DiskSchemaEntry deserialize(InputStream inputStream) throws IOException {
    DiskSchemaEntry res = new DiskSchemaEntry();
    res.deviceID = ReadWriteIOUtils.readString(inputStream);
    res.seriesKey = ReadWriteIOUtils.readString(inputStream);
    res.measurementName = ReadWriteIOUtils.readString(inputStream);
    res.type = ReadWriteIOUtils.readByte(inputStream);
    res.encoding = ReadWriteIOUtils.readByte(inputStream);
    res.compressor = ReadWriteIOUtils.readByte(inputStream);
    res.isAligned = ReadWriteIOUtils.readBool(inputStream);
    // read byte len
    res.entrySize = ReadWriteIOUtils.readInt(inputStream);
    res.entrySize += Integer.BYTES;
    if (isTombstone(res.deviceID)) res.deviceID = TOMBSTONE;
    return res;
  }

  private static Boolean isTombstone(String deviceID) {
    int length = deviceID.getBytes().length;
    byte[] bytes = new byte[length];
    return deviceID.equals(new String(bytes, 0, length));
  }

  @Override
  public String toString() {
    return "DiskSchemaEntry{"
        + "deviceID='"
        + deviceID
        + '\''
        + ", seriesKey='"
        + seriesKey
        + '\''
        + ", type="
        + type
        + ", encoding="
        + encoding
        + ", compressor="
        + compressor
        + '}';
  }
}
