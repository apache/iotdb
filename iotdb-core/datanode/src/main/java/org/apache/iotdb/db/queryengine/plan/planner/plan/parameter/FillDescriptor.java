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

package org.apache.iotdb.db.queryengine.plan.planner.plan.parameter;

import org.apache.iotdb.db.queryengine.plan.statement.component.FillPolicy;
import org.apache.iotdb.db.queryengine.plan.statement.literal.Literal;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.TimeDuration;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class FillDescriptor {

  // policy of fill null values
  private final FillPolicy fillPolicy;

  // filled value when fillPolicy is VALUE
  private final Literal fillValue;

  // if
  private final TimeDuration timeDurationThreshold;

  public FillDescriptor(
      FillPolicy fillPolicy, Literal fillValue, TimeDuration timeDurationThreshold) {
    this.fillPolicy = fillPolicy;
    this.fillValue = fillValue;
    this.timeDurationThreshold = timeDurationThreshold;
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(fillPolicy.ordinal(), byteBuffer);
    if (fillPolicy == FillPolicy.CONSTANT) {
      fillValue.serialize(byteBuffer);
    }
    ReadWriteIOUtils.write(timeDurationThreshold != null, byteBuffer);
    if (timeDurationThreshold != null) {
      timeDurationThreshold.serialize(byteBuffer);
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(fillPolicy.ordinal(), stream);
    if (fillPolicy == FillPolicy.CONSTANT) {
      fillValue.serialize(stream);
    }
    ReadWriteIOUtils.write(timeDurationThreshold != null, stream);
    if (timeDurationThreshold != null) {
      timeDurationThreshold.serialize(stream);
    }
  }

  public static FillDescriptor deserialize(ByteBuffer byteBuffer) {
    FillPolicy fillPolicy = FillPolicy.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    Literal fillValue = null;
    if (fillPolicy == FillPolicy.CONSTANT) {
      fillValue = Literal.deserialize(byteBuffer);
    }
    boolean hasTimeDurationThreshold = ReadWriteIOUtils.readBool(byteBuffer);
    TimeDuration timeDurationThreshold = null;
    if (hasTimeDurationThreshold) {
      timeDurationThreshold = TimeDuration.deserialize(byteBuffer);
    }
    return new FillDescriptor(fillPolicy, fillValue, timeDurationThreshold);
  }

  public FillPolicy getFillPolicy() {
    return fillPolicy;
  }

  public Literal getFillValue() {
    return fillValue;
  }

  public TimeDuration getTimeDurationThreshold() {
    return timeDurationThreshold;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FillDescriptor that = (FillDescriptor) o;
    return fillPolicy == that.fillPolicy && Objects.equals(fillValue, that.fillValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fillPolicy, fillValue);
  }
}
