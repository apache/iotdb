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

package org.apache.iotdb.commons.consensus.index.impl;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.ProgressIndexType;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class MinimumProgressIndex extends ProgressIndex {

  public static final MinimumProgressIndex INSTANCE = new MinimumProgressIndex();
  private static final TotalOrderSumTuple TOTAL_ORDER_SUM_TUPLE = new TotalOrderSumTuple();

  private MinimumProgressIndex() {}

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    ProgressIndexType.MINIMUM_PROGRESS_INDEX.serialize(byteBuffer);
  }

  @Override
  public void serialize(OutputStream stream) throws IOException {
    ProgressIndexType.MINIMUM_PROGRESS_INDEX.serialize(stream);
  }

  @Override
  public boolean isAfter(@Nonnull ProgressIndex progressIndex) {
    return false;
  }

  @Override
  public boolean equals(ProgressIndex progressIndex) {
    return progressIndex instanceof MinimumProgressIndex;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    return getClass() == obj.getClass();
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public ProgressIndex deepCopy() {
    return INSTANCE;
  }

  @Override
  public ProgressIndex updateToMinimumEqualOrIsAfterProgressIndex(ProgressIndex progressIndex) {
    return progressIndex == null ? this : progressIndex.deepCopy();
  }

  @Override
  public ProgressIndexType getType() {
    return ProgressIndexType.MINIMUM_PROGRESS_INDEX;
  }

  @Override
  public TotalOrderSumTuple getTotalOrderSumTuple() {
    return TOTAL_ORDER_SUM_TUPLE;
  }

  public static MinimumProgressIndex deserializeFrom(ByteBuffer byteBuffer) {
    return INSTANCE;
  }

  public static MinimumProgressIndex deserializeFrom(InputStream stream) {
    return INSTANCE;
  }

  @Override
  public String toString() {
    return "MinimumProgressIndex{}";
  }
}
