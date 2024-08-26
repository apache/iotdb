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

package org.apache.iotdb.db.queryengine.execution.operator.window.ainode;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class CountInferenceWindowParameter extends InferenceWindowParameter {

  private final long interval;
  private final long step;

  public CountInferenceWindowParameter(long interval, long step) {
    this.windowType = InferenceWindowType.COUNT;
    this.interval = interval;
    this.step = step;
  }

  public long getInterval() {
    return interval;
  }

  public long getStep() {
    return step;
  }

  @Override
  public void serializeAttributes(ByteBuffer buffer) {
    ReadWriteIOUtils.write(interval, buffer);
    ReadWriteIOUtils.write(step, buffer);
  }

  @Override
  public void serializeAttributes(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(interval, stream);
    ReadWriteIOUtils.write(step, stream);
  }

  public static CountInferenceWindowParameter deserialize(ByteBuffer byteBuffer) {
    long interval = ReadWriteIOUtils.readLong(byteBuffer);
    long step = ReadWriteIOUtils.readLong(byteBuffer);
    return new CountInferenceWindowParameter(interval, step);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof CountInferenceWindowParameter)) {
      return false;
    }
    CountInferenceWindowParameter parameter = (CountInferenceWindowParameter) obj;
    return interval == parameter.interval && step == parameter.step;
  }

  @Override
  public int hashCode() {
    return Objects.hash(interval, step);
  }
}
