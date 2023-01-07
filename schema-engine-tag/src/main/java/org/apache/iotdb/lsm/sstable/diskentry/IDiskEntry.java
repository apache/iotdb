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
package org.apache.iotdb.lsm.sstable.diskentry;

import org.apache.iotdb.lsm.sstable.fileIO.ISSTableInputStream;
import org.apache.iotdb.lsm.sstable.fileIO.ISSTableOutputStream;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Represents a disk entry, which implements the disk data structure of this interface, and can be
 * read and written using ISSTableInputStream and ISSTableOutputStream
 *
 * @see ISSTableInputStream
 * @see ISSTableOutputStream
 */
public interface IDiskEntry {

  /**
   * Serialize to output stream
   *
   * @param out data output stream
   * @return serialized size
   * @throws IOException if an I/O error occurs.
   */
  int serialize(DataOutputStream out) throws IOException;

  /**
   * Deserialize from input stream
   *
   * @param input data input stream
   * @return disk entry
   * @throws IOException if an I/O error occurs.
   */
  IDiskEntry deserialize(DataInputStream input) throws IOException;
}
