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

package org.apache.iotdb.udf.api.relational.table.argument;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface Argument {

  void serialize(ByteBuffer buffer);

  void serialize(DataOutputStream buffer) throws IOException;

  static Argument deserialize(ByteBuffer buffer) {
    ArgumentType type = ArgumentType.values()[buffer.getInt()];
    switch (type) {
      case TABLE_ARGUMENT:
        return TableArgument.deserialize(buffer);
      case SCALAR_ARGUMENT:
        return ScalarArgument.deserialize(buffer);
      default:
        throw new IllegalArgumentException("Unknown argument type: " + type);
    }
  }

  enum ArgumentType {
    TABLE_ARGUMENT,
    SCALAR_ARGUMENT
  }
}
