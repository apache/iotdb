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

package org.apache.iotdb.db.pipe.processor.aggregate.datastructure.intermediate;

import java.nio.ByteBuffer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * The attributes of an intermediate result. There shall be a one-to-one match between an
 * intermediate result's static attributes and its name.
 */
public class IntermediateResultAttributes {
  /** The updateFunction receives an object to update its value. */
  private final UnaryOperator<Object> updateFunction;

  /**
   * The serializationFunction serialize an object to bytebuffer. Note that a serializable
   * intermediate result shall have different name from an un-serializable one
   */
  private Function<Object, ByteBuffer> serializeFunction;

  /**
   * The deserializationFunction deserialize an object from bytebuffer. Note that a serializable
   * intermediate result shall have different name from an un-serializable one
   */
  private Function<ByteBuffer, Object> deserializeFunction;

  public IntermediateResultAttributes(UnaryOperator<Object> updateFunction) {
    this.updateFunction = updateFunction;
  }

  public IntermediateResultAttributes(
      UnaryOperator<Object> updateFunction,
      Function<Object, ByteBuffer> serializeFunction,
      Function<ByteBuffer, Object> deserializeFunction) {
    this.updateFunction = updateFunction;
    this.serializeFunction = serializeFunction;
    this.deserializeFunction = deserializeFunction;
  }

  public UnaryOperator<Object> getUpdateFunction() {
    return updateFunction;
  }

  public Function<Object, ByteBuffer> getSerializeFunction() {
    return serializeFunction;
  }

  public Function<ByteBuffer, Object> getDeserializeFunction() {
    return deserializeFunction;
  }
}
