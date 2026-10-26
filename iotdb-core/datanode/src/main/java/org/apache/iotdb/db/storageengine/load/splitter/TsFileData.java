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
   * @param includeContent whether to inline chunk payloads; false keeps a {@link ChunkPayloadRef}
   *     for a chunk already written into the staged TsFile, so no payload is stored twice
   */
  void serialize(DataOutputStream stream, boolean includeContent) throws IOException;

  /**
   * The bytes {@link #serialize(DataOutputStream, boolean)} writes, calculated without serializing
   * anything, so that a size can be asked for without allocating a buffer to measure.
   *
   * <p>It describes the state the record is in: one that still holds decoded pages is encoded by
   * {@link #serialize} itself, so it is sized once its pages have been ended, which is the state a
   * piece arrives at the WAL in.
   */
  int serializedSize(boolean includeContent);

  static TsFileData deserialize(InputStream stream)
      throws IOException, PageException, IllegalPathException {
    final int typeOrdinal = ReadWriteIOUtils.readInt(stream);
    if (typeOrdinal < 0 || typeOrdinal >= TsFileDataType.values().length) {
      throw new IOException(
          String.format(
              StorageEngineMessages
                  .EXCEPTION_UNKNOWN_TSFILEDATA_TYPE_ORDINAL_ARG_WHEN_DESERIALIZING_A_LOAD_PIECE_27A777D2,
              typeOrdinal));
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
