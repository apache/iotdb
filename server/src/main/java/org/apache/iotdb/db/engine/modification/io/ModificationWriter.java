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

package org.apache.iotdb.db.engine.modification.io;

import org.apache.iotdb.db.engine.modification.Modification;

import java.io.IOException;

/**
 * ModificationWriter provides methods for writing a modification to a persistent medium like file
 * system.
 */
public interface ModificationWriter {

  /**
   * Write a new modification to the persistent medium. Notice that after calling write(), a
   * fileWriter is opened,
   *
   * @param mod the modification to be written.
   */
  void write(Modification mod) throws IOException;

  /** Release resources like streams. */
  void close() throws IOException;

  /** Abort last modification. */
  void abort() throws IOException;
}
