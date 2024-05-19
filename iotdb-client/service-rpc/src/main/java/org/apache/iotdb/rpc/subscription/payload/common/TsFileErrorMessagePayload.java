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

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class TsFileErrorMessagePayload implements SubscriptionMessagePayload {

  private transient String errorMessage;

  private transient boolean critical;

  public String getErrorMessage() {
    return errorMessage;
  }

  public boolean isCritical() {
    return critical;
  }

  public TsFileErrorMessagePayload() {}

  public TsFileErrorMessagePayload(String errorMessage, boolean critical) {
    this.errorMessage = errorMessage;
    this.critical = critical;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(errorMessage, stream);
    ReadWriteIOUtils.write(critical, stream);
  }

  @Override
  public SubscriptionMessagePayload deserialize(ByteBuffer buffer) {
    this.errorMessage = ReadWriteIOUtils.readString(buffer);
    this.critical = ReadWriteIOUtils.readBool(buffer);
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
    final TsFileErrorMessagePayload that = (TsFileErrorMessagePayload) obj;
    return Objects.equals(this.errorMessage, that.errorMessage)
        && Objects.equals(this.critical, that.critical);
  }

  @Override
  public int hashCode() {
    return Objects.hash(errorMessage, critical);
  }

  @Override
  public String toString() {
    return "TsFileErrorMessagePayload{errorMessage="
        + errorMessage
        + ", critical="
        + critical
        + "}";
  }
}
