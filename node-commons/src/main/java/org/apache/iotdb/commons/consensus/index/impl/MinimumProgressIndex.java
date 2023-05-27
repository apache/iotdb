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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class MinimumProgressIndex implements ProgressIndex {

  public MinimumProgressIndex() {}

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    ProgressIndexType.MINIMUM_CONSENSUS_INDEX.serialize(byteBuffer);
  }

  @Override
  public void serialize(OutputStream stream) throws IOException {
    ProgressIndexType.MINIMUM_CONSENSUS_INDEX.serialize(stream);
  }

  @Override
  public boolean isAfter(ProgressIndex progressIndex) {
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
  public ProgressIndex updateToMinimumIsAfterProgressIndex(ProgressIndex progressIndex) {
    return progressIndex == null ? this : progressIndex;
  }

  public static MinimumProgressIndex deserializeFrom(ByteBuffer byteBuffer) {
    return new MinimumProgressIndex();
  }

  public static MinimumProgressIndex deserializeFrom(InputStream stream) {
    return new MinimumProgressIndex();
  }

  @Override
  public String toString() {
    return "MinimumProgressIndex{}";
  }
}
