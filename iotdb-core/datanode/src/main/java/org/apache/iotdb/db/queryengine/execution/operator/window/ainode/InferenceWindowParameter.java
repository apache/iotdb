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

import org.apache.iotdb.db.exception.sql.SemanticException;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class InferenceWindowParameter {

  protected InferenceWindowType windowType;

  public InferenceWindowType getWindowType() {
    return windowType;
  }

  public abstract void serializeAttributes(ByteBuffer buffer);

  public abstract void serializeAttributes(DataOutputStream stream) throws IOException;

  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(windowType.ordinal(), buffer);
    serializeAttributes(buffer);
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(windowType.ordinal(), stream);
    serializeAttributes(stream);
  }

  public static InferenceWindowParameter deserialize(ByteBuffer byteBuffer) {
    InferenceWindowType windowType =
        InferenceWindowType.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    if (windowType == InferenceWindowType.TAIL) {
      return BottomInferenceWindowParameter.deserialize(byteBuffer);
    } else if (windowType == InferenceWindowType.COUNT) {
      return CountInferenceWindowParameter.deserialize(byteBuffer);
    } else {
      throw new SemanticException("Unsupported inference window type: " + windowType);
    }
  }
}
