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

import org.apache.iotdb.commons.consensus.index.impl.HybridProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.IoTProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.KafkaProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MetaProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.RecoverProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.StateProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.TimeWindowStateProgressIndex;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public enum ProgressIndexType {
  MINIMUM_PROGRESS_INDEX((short) 1),
  IOT_PROGRESS_INDEX((short) 2),
  SIMPLE_PROGRESS_INDEX((short) 3),
  RECOVER_PROGRESS_INDEX((short) 4),
  HYBRID_PROGRESS_INDEX((short) 5),
  META_PROGRESS_INDEX((short) 6),
  TIME_WINDOW_STATE_PROGRESS_INDEX((short) 7),
  STATE_PROGRESS_INDEX((short) 8),
  KAFKA_PROGRESS_INDEX((short) 9);
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
    final short indexType = ReadWriteIOUtils.readShort(byteBuffer);
    switch (indexType) {
      case 1:
        return MinimumProgressIndex.deserializeFrom(byteBuffer);
      case 2:
        return IoTProgressIndex.deserializeFrom(byteBuffer);
      case 3:
        return SimpleProgressIndex.deserializeFrom(byteBuffer);
      case 4:
        return RecoverProgressIndex.deserializeFrom(byteBuffer);
      case 5:
        return HybridProgressIndex.deserializeFrom(byteBuffer);
      case 6:
        return MetaProgressIndex.deserializeFrom(byteBuffer);
      case 7:
        return TimeWindowStateProgressIndex.deserializeFrom(byteBuffer);
      case 8:
        return StateProgressIndex.deserializeFrom(byteBuffer);
      case 9:
        return KafkaProgressIndex.deserializeFrom(byteBuffer);
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported progress index type %s.", indexType));
    }
  }

  public static ProgressIndex deserializeFrom(InputStream stream) throws IOException {
    final short indexType = ReadWriteIOUtils.readShort(stream);
    switch (indexType) {
      case 1:
        return MinimumProgressIndex.deserializeFrom(stream);
      case 2:
        return IoTProgressIndex.deserializeFrom(stream);
      case 3:
        return SimpleProgressIndex.deserializeFrom(stream);
      case 4:
        return RecoverProgressIndex.deserializeFrom(stream);
      case 5:
        return HybridProgressIndex.deserializeFrom(stream);
      case 6:
        return MetaProgressIndex.deserializeFrom(stream);
      case 7:
        return TimeWindowStateProgressIndex.deserializeFrom(stream);
      case 8:
        return StateProgressIndex.deserializeFrom(stream);
      case 9:
        return KafkaProgressIndex.deserializeFrom(stream);
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported progress index type %s.", indexType));
    }
  }
}
