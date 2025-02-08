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

package org.apache.iotdb.db.storageengine.dataregion.modification.v1.io;

import org.apache.iotdb.db.storageengine.dataregion.modification.v1.Modification;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

/** ModificationReader reads all modifications from a persistent medium like file system. */
public interface ModificationReader {

  /**
   * Read all modifications from a persistent medium. If the mods file is crashed, the redundant
   * modifications will be truncated until the file is correct.
   *
   * @return a list of modifications contained the medium.
   */
  Collection<Modification> read();

  /**
   * Get an iterator over this mod file, others keep consistence with {@link #read()}. Please ensure
   * you have called hasNext() with return of {@code true} before calling next().
   *
   * @return the modification iterator.
   */
  Iterator<Modification> getModificationIterator();

  /** Release resources like streams. */
  void close() throws IOException;
}
