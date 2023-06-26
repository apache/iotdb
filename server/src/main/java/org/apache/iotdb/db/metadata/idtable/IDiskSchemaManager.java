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

package org.apache.iotdb.db.metadata.idtable;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.metadata.idtable.entry.DiskSchemaEntry;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/** This class manages IO of id table's schema entry. */
public interface IDiskSchemaManager {

  /**
   * Serialize a disk schema entry.
   *
   * @param schemaEntry disk schema entry
   * @return disk position of that entry
   */
  long serialize(DiskSchemaEntry schemaEntry);

  /**
   * Recover id table from log file.
   *
   * @param idTable id table need to be recovered
   */
  void recover(IDTable idTable);

  /**
   * Get all disk schema entries from file.
   *
   * @return collection of all disk schema entires
   * @throws IOException IOException
   */
  @TestOnly
  Collection<DiskSchemaEntry> getAllSchemaEntry() throws IOException;

  /**
   * Get DiskSchemaEntries from disk file.
   *
   * @param offsets the offset of each record on the disk file
   * @return DiskSchemaEntries
   */
  List<DiskSchemaEntry> getDiskSchemaEntriesByOffset(List<Long> offsets);

  /**
   * Delete DiskSchemaEntries on disk.
   *
   * @param offset the offset of a record on the disk file
   * @throws MetadataException metadata exception
   */
  void deleteDiskSchemaEntryByOffset(long offset) throws MetadataException;

  /**
   * Close file and free resource.
   *
   * @throws IOException IOException
   */
  void close() throws IOException;
}
