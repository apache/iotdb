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
  public String deviceID;

  public String seriesKey;

  public long flushTime;

  public byte type;

  public byte encoding;

  public byte compressor;

  private DiskSchemaEntry() {}

  public DiskSchemaEntry(
      String deviceID,
      String seriesKey,
      long flushTime,
      byte type,
      byte encoding,
      byte compressor) {
    this.deviceID = deviceID;
    this.seriesKey = seriesKey;
    this.flushTime = flushTime;
    this.type = type;
    this.encoding = encoding;
    this.compressor = compressor;
  }

  public int serialize(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(deviceID, outputStream);
    byteLen += ReadWriteIOUtils.write(seriesKey, outputStream);
    byteLen += ReadWriteIOUtils.write(flushTime, outputStream);
    byteLen += ReadWriteIOUtils.write(type, outputStream);
    byteLen += ReadWriteIOUtils.write(encoding, outputStream);
    byteLen += ReadWriteIOUtils.write(compressor, outputStream);
    byteLen += ReadWriteIOUtils.write(byteLen, outputStream);

    return byteLen;
  }

  public static DiskSchemaEntry deserialize(InputStream inputStream) throws IOException {
    DiskSchemaEntry res = new DiskSchemaEntry();
    res.deviceID = ReadWriteIOUtils.readString(inputStream);
    res.seriesKey = ReadWriteIOUtils.readString(inputStream);
    res.flushTime = ReadWriteIOUtils.readLong(inputStream);
    res.type = ReadWriteIOUtils.readByte(inputStream);
    res.encoding = ReadWriteIOUtils.readByte(inputStream);
    res.compressor = ReadWriteIOUtils.readByte(inputStream);
    // read byte len
    ReadWriteIOUtils.readInt(inputStream);

    return res;
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
        + ", flushTime="
        + flushTime
        + ", type="
        + type
        + ", encoding="
        + encoding
        + ", compressor="
        + compressor
        + '}';
  }
}
