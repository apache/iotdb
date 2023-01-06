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
package org.apache.iotdb.db.metadata.storagegroup;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;

import java.io.IOException;
import java.util.List;

// This class declares all the interfaces for database management.
public interface IStorageGroupSchemaManager {

  void init() throws MetadataException, IOException;

  void clear() throws IOException;

  /**
   * create database of the given path to MTree.
   *
   * @param path database path
   */
  void setStorageGroup(PartialPath path) throws MetadataException;

  /**
   * Get database name by path
   *
   * <p>e.g., root.sg1 is a database and path = root.sg1.d1, return root.sg1
   *
   * @param path only full path, cannot be path pattern
   * @return database in the given path
   */
  PartialPath getBelongedStorageGroup(PartialPath path) throws StorageGroupNotSetException;

  List<PartialPath> getInvolvedStorageGroups(PartialPath pathPattern) throws MetadataException;
}
