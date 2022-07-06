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

package org.apache.iotdb.db.mpp.plan.planner.plan.parameter;

import org.apache.iotdb.db.mpp.plan.statement.component.FillPolicy;
import org.apache.iotdb.db.mpp.plan.statement.literal.Literal;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class FillDescriptor {

  // policy of fill null values
  private final FillPolicy fillPolicy;

  // filled value when fillPolicy is VALUE
  private Literal fillValue;

  public FillDescriptor(FillPolicy fillPolicy) {
    this.fillPolicy = fillPolicy;
  }

  public FillDescriptor(FillPolicy fillPolicy, Literal fillValue) {
    this.fillPolicy = fillPolicy;
    this.fillValue = fillValue;
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(fillPolicy.ordinal(), byteBuffer);
    if (fillPolicy == FillPolicy.VALUE) {
      fillValue.serialize(byteBuffer);
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(fillPolicy.ordinal(), stream);
    if (fillPolicy == FillPolicy.VALUE) {
      fillValue.serialize(stream);
    }
  }

  public static FillDescriptor deserialize(ByteBuffer byteBuffer) {
    FillPolicy fillPolicy = FillPolicy.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    if (fillPolicy == FillPolicy.VALUE) {
      Literal fillValue = Literal.deserialize(byteBuffer);
      return new FillDescriptor(fillPolicy, fillValue);
    } else {
      return new FillDescriptor(fillPolicy);
    }
  }

  public FillPolicy getFillPolicy() {
    return fillPolicy;
  }

  public Literal getFillValue() {
    return fillValue;
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
