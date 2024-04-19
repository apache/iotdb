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

package org.apache.iotdb.rpc.subscription.payload.common;

import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public class TsFilePieceMessagePayload implements SubscriptionMessagePayload {

  private transient String fileName;

  private transient long nextWritingOffset;

  private transient byte[] filePiece;

  public String getFileName() {
    return fileName;
  }

  public long getNextWritingOffset() {
    return nextWritingOffset;
  }

  public byte[] getFilePiece() {
    return filePiece;
  }

  public TsFilePieceMessagePayload() {}

  public TsFilePieceMessagePayload(String fileName, long nextWritingOffset, byte[] filePiece) {
    this.fileName = fileName;
    this.nextWritingOffset = nextWritingOffset;
    this.filePiece = filePiece;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(fileName, stream);
    ReadWriteIOUtils.write(nextWritingOffset, stream);
    ReadWriteIOUtils.write(new Binary(filePiece), stream);
  }

  @Override
  public SubscriptionMessagePayload deserialize(ByteBuffer buffer) {
    this.fileName = ReadWriteIOUtils.readString(buffer);
    this.nextWritingOffset = ReadWriteIOUtils.readLong(buffer);
    final int size = ReadWriteIOUtils.readInt(buffer);
    this.filePiece = ReadWriteIOUtils.readBytes(buffer, size);
    return this;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final TsFilePieceMessagePayload that = (TsFilePieceMessagePayload) obj;
    return Objects.equals(this.fileName, that.fileName)
        && Objects.equals(this.nextWritingOffset, that.nextWritingOffset)
        && Arrays.equals(this.filePiece, that.filePiece);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileName, nextWritingOffset, Arrays.hashCode(filePiece));
  }

  @Override
  public String toString() {
    return "TsFilePieceMessagePayload{fileName="
        + fileName
        + ", nextWritingOffset="
        + nextWritingOffset
        + ", filePiece="
        + Arrays.toString(filePiece)
        + "}";
  }
}
