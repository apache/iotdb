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
package org.apache.iotdb.db.wal.buffer;

import org.apache.iotdb.db.utils.TestOnly;

/** Currently, there are 2 buffer types */
public interface IWALBuffer extends AutoCloseable {
  /**
   * Write WALEdit into wal buffer.
   *
   * @param edit info will be written into wal buffer
   */
  void write(WALEdit edit);

  /** Get current log version id */
  int getCurrentLogVersion();

  @Override
  void close();

  @TestOnly
  boolean isAllWALEditConsumed();
}
