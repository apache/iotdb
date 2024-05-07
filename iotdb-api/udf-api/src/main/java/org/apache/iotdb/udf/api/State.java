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

package org.apache.iotdb.udf.api;

public interface State {
  /** Reset your state object to its initial state. */
  void reset();

  /**
   * Serialize your state into byte array. The order of serialization must be consistent with
   * deserialization.
   */
  byte[] serialize();

  /**
   * Deserialize byte array into your state. The order of deserialization must be consistent with
   * serialization.
   */
  void deserialize(byte[] bytes);

  /** Destroy state. You may release previously binding resource in this method. */
  default void destroyState() {}
  ;
}
