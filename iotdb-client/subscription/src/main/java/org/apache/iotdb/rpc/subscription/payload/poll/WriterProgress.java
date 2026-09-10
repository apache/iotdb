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

public class WriterProgress {

  private final long physicalTime;
  private final long localSeq;

  public WriterProgress(final long physicalTime, final long localSeq) {
    this.physicalTime = physicalTime;
    this.localSeq = localSeq;
  }

  public long getPhysicalTime() {
    return physicalTime;
  }

  public long getLocalSeq() {
    return localSeq;
  }

  public void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(physicalTime, stream);
    ReadWriteIOUtils.write(localSeq, stream);
  }

  public static WriterProgress deserialize(final ByteBuffer buffer) {
    return new WriterProgress(ReadWriteIOUtils.readLong(buffer), ReadWriteIOUtils.readLong(buffer));
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof WriterProgress)) {
      return false;
    }
    final WriterProgress that = (WriterProgress) obj;
    return physicalTime == that.physicalTime && localSeq == that.localSeq;
  }

  @Override
  public int hashCode() {
    return Objects.hash(physicalTime, localSeq);
  }

  @Override
  public String toString() {
    return "WriterProgress{" + "physicalTime=" + physicalTime + ", localSeq=" + localSeq + '}';
  }
}
