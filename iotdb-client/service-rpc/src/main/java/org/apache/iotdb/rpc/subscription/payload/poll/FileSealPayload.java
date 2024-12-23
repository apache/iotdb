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

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class FileSealPayload implements SubscriptionPollPayload {

  /** The name of the file to be sealed. */
  private transient String fileName;

  /** The length of the file. */
  private transient long fileLength;

  public String getFileName() {
    return fileName;
  }

  public long getFileLength() {
    return fileLength;
  }

  public FileSealPayload() {}

  public FileSealPayload(final String fileName, final long fileLength) {
    this.fileName = fileName;
    this.fileLength = fileLength;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(fileName, stream);
    ReadWriteIOUtils.write(fileLength, stream);
  }

  @Override
  public SubscriptionPollPayload deserialize(final ByteBuffer buffer) {
    this.fileName = ReadWriteIOUtils.readString(buffer);
    this.fileLength = ReadWriteIOUtils.readLong(buffer);
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
    final FileSealPayload that = (FileSealPayload) obj;
    return Objects.equals(this.fileName, that.fileName)
        && Objects.equals(this.fileLength, that.fileLength);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileName, fileLength);
  }

  @Override
  public String toString() {
    return "FileSealPayload{fileName=" + fileName + ", fileLength=" + fileLength + "}";
  }
}
