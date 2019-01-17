/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.engine.overflow.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class OFFileMetadata {

  private long lastFooterOffset;
  private List<OFRowGroupListMetadata> rowGroupLists;

  public OFFileMetadata() {
  }

  public OFFileMetadata(long lastFooterOffset, List<OFRowGroupListMetadata> rowGroupLists) {
    this.lastFooterOffset = lastFooterOffset;
    this.rowGroupLists = rowGroupLists;
  }

  /**
   * function for deserializing data from input stream.
   */
  public static OFFileMetadata deserializeFrom(InputStream inputStream) throws IOException {
    long lastFooterOffset = ReadWriteIOUtils.readLong(inputStream);
    int size = ReadWriteIOUtils.readInt(inputStream);
    List<OFRowGroupListMetadata> list = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      list.add(OFRowGroupListMetadata.deserializeFrom(inputStream));
    }
    return new OFFileMetadata(lastFooterOffset, list);
  }

  public static OFFileMetadata deserializeFrom(ByteBuffer buffer) throws IOException {
    throw new NotImplementedException();
  }

  /**
   * add OFRowGroupListMetadata to list.
   */
  public void addRowGroupListMetaData(OFRowGroupListMetadata rowGroupListMetadata) {
    if (rowGroupLists == null) {
      rowGroupLists = new ArrayList<OFRowGroupListMetadata>();
    }
    rowGroupLists.add(rowGroupListMetadata);
  }

  public List<OFRowGroupListMetadata> getRowGroupLists() {
    return rowGroupLists == null ? null : Collections.unmodifiableList(rowGroupLists);
  }

  public long getLastFooterOffset() {
    return lastFooterOffset;
  }

  public void setLastFooterOffset(long lastFooterOffset) {
    this.lastFooterOffset = lastFooterOffset;
  }

  @Override
  public String toString() {
    return String.format("OFFileMetadata{ last offset: %d, RowGroupLists: %s }", lastFooterOffset,
        rowGroupLists.toString());
  }

  /**
   * function for serializing data to output stream.
   */
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(lastFooterOffset, outputStream);
    int size = rowGroupLists.size();
    byteLen += ReadWriteIOUtils.write(size, outputStream);
    for (OFRowGroupListMetadata ofRowGroupListMetadata : rowGroupLists) {
      byteLen += ofRowGroupListMetadata.serializeTo(outputStream);
    }
    return byteLen;
  }

  public int serializeTo(ByteBuffer buffer) throws IOException {
    throw new NotImplementedException();
  }

}
