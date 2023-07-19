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
package org.apache.iotdb.lsm.sstable.index;

import java.io.IOException;
import java.util.Map;

/** Used to generate a disk index for records and write to disk */
public interface IDiskIndexWriter {

  /**
   * generate a disk index for records and write to disk
   *
   * @param records a map that holds all records, the map can be unordered
   * @param ordered whether the map is in order
   * @return start offset of the disk index
   * @throws IOException if an I/O error occurs.
   */
  long write(Map<String, Long> records, boolean ordered) throws IOException;

  /**
   * close resource
   *
   * @throws IOException if an I/O error occurs.
   */
  void close() throws IOException;
}
