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

package org.apache.iotdb.commons.consensus.index;

import org.apache.iotdb.commons.consensus.index.impl.IoTProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public enum ProgressIndexType {
  MINIMUM_CONSENSUS_INDEX((short) 1),
  IOT_CONSENSUS_INDEX((short) 2),
  ;

  private final short type;

  ProgressIndexType(short type) {
    this.type = type;
  }

  public short getType() {
    return type;
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(type, byteBuffer);
  }

  public void serialize(OutputStream stream) throws IOException {
    ReadWriteIOUtils.write(type, stream);
  }

  public static ProgressIndex deserializeFrom(ByteBuffer byteBuffer) {
    short indexType = byteBuffer.getShort();
    switch (indexType) {
      case 1:
        return MinimumProgressIndex.deserializeFrom(byteBuffer);
      case 2:
        return IoTProgressIndex.deserializeFrom(byteBuffer);
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported Consensus Index type %s.", indexType));
    }
  }

  public static ProgressIndex deserializeFrom(InputStream stream) throws IOException {
    short indexType = ReadWriteIOUtils.readShort(stream);
    switch (indexType) {
      case 1:
        return MinimumProgressIndex.deserializeFrom(stream);
      case 2:
        return IoTProgressIndex.deserializeFrom(stream);
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported Consensus Index type %s.", indexType));
    }
  }
}
