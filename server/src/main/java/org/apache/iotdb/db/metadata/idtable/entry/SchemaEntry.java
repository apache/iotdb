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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.idtable.IDiskSchemaManager;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.Objects;

import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;

/**
 * Schema entry of id table <br>
 * Notice that this class is also a last cache container for last cache
 */
public class SchemaEntry {

  /* 40 bits of disk pointer */
  /*  1 byte of compressor  */
  /*   1 byte of encoding   */
  /*    1 byte of type      */
  private long schema;

  /** This static field will not occupy memory */
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  @TestOnly
  public SchemaEntry(TSDataType dataType) {
    TSEncoding encoding = getDefaultEncoding(dataType);
    CompressionType compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();

    schema |= dataType.serialize();
    schema |= (((long) encoding.serialize()) << 8);
    schema |= (((long) compressionType.serialize()) << 16);
  }

  // used in recover
  public SchemaEntry(
      TSDataType dataType, TSEncoding encoding, CompressionType compressionType, long diskPos) {
    schema |= dataType.serialize();
    schema |= (((long) encoding.serialize()) << 8);
    schema |= (((long) compressionType.serialize()) << 16);

    schema |= (diskPos << 24);
  }

  public SchemaEntry(
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressionType,
      IDeviceID deviceID,
      PartialPath fullPath,
      boolean isAligned,
      IDiskSchemaManager IDiskSchemaManager) {
    schema |= dataType.serialize();
    schema |= (((long) encoding.serialize()) << 8);
    schema |= (((long) compressionType.serialize()) << 16);

    // write log file
    if (config.isEnableIDTableLogFile()) {
      DiskSchemaEntry diskSchemaEntry =
          new DiskSchemaEntry(
              deviceID.toStringID(),
              fullPath.getFullPath(),
              fullPath.getMeasurement(),
              dataType.serialize(),
              encoding.serialize(),
              compressionType.serialize(),
              isAligned);
      schema |= (IDiskSchemaManager.serialize(diskSchemaEntry) << 24);
    }
  }

  /**
   * get disk pointer of ts from long value of schema
   *
   * @return disk pointer
   */
  public long getDiskPointer() {
    return schema >> 24;
  }

  /**
   * get ts data type from long value of schema
   *
   * @return ts data type
   */
  public TSDataType getTSDataType() {
    return TSDataType.deserialize((byte) schema);
  }

  /**
   * get ts encoding from long value of schema
   *
   * @return ts encoding
   */
  public TSEncoding getTSEncoding() {
    return TSEncoding.deserialize((byte) (schema >> 8));
  }

  /**
   * get compression type from long value of schema
   *
   * @return compression type
   */
  public CompressionType getCompressionType() {
    return CompressionType.deserialize((byte) (schema >> 16));
  }

  @Override
  // Notice that we only compare schema
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SchemaEntry)) {
      return false;
    }
    SchemaEntry that = (SchemaEntry) o;
    return schema == that.schema;
  }

  @Override
  // Notice that we only compare schema
  public int hashCode() {
    return Objects.hash(schema);
  }
  // endregion
}
