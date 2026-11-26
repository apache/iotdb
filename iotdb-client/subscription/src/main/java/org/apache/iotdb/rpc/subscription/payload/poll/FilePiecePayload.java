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

package org.apache.iotdb.rpc.subscription.payload.poll;

import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public class FilePiecePayload implements SubscriptionPollPayload {

  /** The name of the file. */
  private transient String fileName;

  /** The field to be filled in the next {@link PollFilePayload} request. */
  private transient long nextWritingOffset;

  /** The piece of the file content. */
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

  public FilePiecePayload() {}

  public FilePiecePayload(
      final String fileName, final long nextWritingOffset, final byte[] filePiece) {
    this.fileName = fileName;
    this.nextWritingOffset = nextWritingOffset;
    this.filePiece = filePiece;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(fileName, stream);
    ReadWriteIOUtils.write(nextWritingOffset, stream);
    ReadWriteIOUtils.write(new Binary(filePiece), stream);
  }

  @Override
  public SubscriptionPollPayload deserialize(final ByteBuffer buffer) {
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
    final FilePiecePayload that = (FilePiecePayload) obj;
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
    return "FilePiecePayload{fileName="
        + fileName
        + ", nextWritingOffset="
        + nextWritingOffset
        + ", filePiece="
        + formatByteArray(filePiece, 64)
        + "}";
  }

  private static String formatByteArray(final byte[] filePiece, final int maxLength) {
    final int length = filePiece.length;
    final int displayLength = Math.min(length, maxLength);
    final StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int i = 0; i < displayLength; i++) {
      sb.append(String.format("%02x", filePiece[i]));
      if (i < displayLength - 1) {
        sb.append(", ");
      }
    }
    if (length > maxLength) {
      sb.append("...");
    }
    sb.append("]");
    return sb.toString();
  }
}
