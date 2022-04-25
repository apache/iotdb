/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

public class SchemaCacheEntity {
  private final String schemaEntryId;

  private TSDataType tsDataType;

  private TSEncoding tsEncoding;

  private CompressionType compressionType;

  private String alias;

  private boolean isAligned;

  @TestOnly
  public SchemaCacheEntity() {
    this.schemaEntryId = "1";
  }

  public SchemaCacheEntity(String schemaEntryId, TSDataType tsDataType, boolean isAligned) {
    this.schemaEntryId = schemaEntryId;
    this.tsDataType = tsDataType;
    this.isAligned = isAligned;
    this.tsEncoding =
        TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder());
    this.compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();
    this.alias = "";
  }

  public SchemaCacheEntity(
      String schemaEntryId,
      TSDataType tsDataType,
      TSEncoding tsEncoding,
      CompressionType compressionType,
      String alias,
      boolean isAligned) {
    this.schemaEntryId = schemaEntryId;
    this.tsDataType = tsDataType;
    this.tsEncoding = tsEncoding;
    this.compressionType = compressionType;
    this.alias = alias;
    this.isAligned = isAligned;
  }

  public String getSchemaEntryId() {
    return schemaEntryId;
  }

  public TSDataType getTsDataType() {
    return tsDataType;
  }

  public void setTsDataType(TSDataType tsDataType) {
    this.tsDataType = tsDataType;
  }

  public TSEncoding getTsEncoding() {
    return tsEncoding;
  }

  public void setTsEncoding(TSEncoding tsEncoding) {
    this.tsEncoding = tsEncoding;
  }

  public CompressionType getCompressionType() {
    return compressionType;
  }

  public void setCompressionType(CompressionType compressionType) {
    this.compressionType = compressionType;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public boolean isAligned() {
    return isAligned;
  }

  public void setAligned(boolean aligned) {
    isAligned = aligned;
  }
}
