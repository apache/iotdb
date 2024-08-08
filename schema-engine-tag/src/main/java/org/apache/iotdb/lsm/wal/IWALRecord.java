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
package org.apache.iotdb.lsm.wal;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/** represents a wal record, which can be extended to implement more complex wal records */
public interface IWALRecord<K, V> extends Cloneable {

  /**
   * serialize the wal record
   *
   * @param buffer byte buffer
   */
  void serialize(ByteBuffer buffer);

  /**
   * deserialize via input stream
   *
   * @param stream data input stream
   * @throws IOException
   */
  void deserialize(DataInputStream stream) throws IOException;

  // generate wal record using prototyping pattern
  IWALRecord clone();

  List<K> getKeys();

  V getValue();
}
