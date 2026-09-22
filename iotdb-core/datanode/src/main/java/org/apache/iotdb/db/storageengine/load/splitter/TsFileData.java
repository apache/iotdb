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

package org.apache.iotdb.db.storageengine.load.splitter;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.i18n.StorageEngineMessages;

import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

public interface TsFileData {
  long getDataSize();

  TsFileDataType getType();

  void serialize(DataOutputStream stream) throws IOException;

  /**
   * Serializes this data.
   *
   * @param includeContent whether chunk payloads must be inlined. When false, a chunk that was
   *     already written into the staged TsFile carries a {@link ChunkPayloadRef} instead of its
   *     bytes, so that the WAL does not store the same payload twice.
   */
  void serialize(DataOutputStream stream, boolean includeContent) throws IOException;

  static TsFileData deserialize(InputStream stream)
      throws IOException, PageException, IllegalPathException {
    final int typeOrdinal = ReadWriteIOUtils.readInt(stream);
    if (typeOrdinal < 0 || typeOrdinal >= TsFileDataType.values().length) {
      throw new IOException();
    }
    final TsFileDataType type = TsFileDataType.values()[typeOrdinal];
    switch (type) {
      case CHUNK:
        return ChunkData.deserialize(stream);
      case DELETION:
        return DeletionData.deserialize(stream);
      default:
        throw new UnsupportedOperationException(
            StorageEngineMessages.UNKNOWN_TSFILE_DATA_TYPE + type);
    }
  }
}
