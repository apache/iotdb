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
import org.apache.iotdb.db.metadata.mtree.ConfigMTree;

import java.io.IOException;
import java.util.List;

// Never support restart
// This class implements all the interfaces for database management. The MTreeAboveSg is used
// to manage all the databases and MNodes above database.
@Deprecated
public class StorageGroupSchemaManager implements IStorageGroupSchemaManager {

  private ConfigMTree mtree;

  private static class StorageGroupManagerHolder {

    private static final StorageGroupSchemaManager INSTANCE = new StorageGroupSchemaManager();

    private StorageGroupManagerHolder() {}
  }

  public static StorageGroupSchemaManager getInstance() {
    return StorageGroupManagerHolder.INSTANCE;
  }

  private StorageGroupSchemaManager() {}

  public synchronized void init() throws MetadataException, IOException {

    mtree = new ConfigMTree();
  }

  public synchronized void clear() throws IOException {

    if (mtree != null) {
      mtree.clear();
    }
  }

  @Override
  public void setStorageGroup(PartialPath path) throws MetadataException {
    mtree.setStorageGroup(path);
  }

  @Override
  public PartialPath getBelongedStorageGroup(PartialPath path) throws StorageGroupNotSetException {
    return mtree.getBelongedStorageGroup(path);
  }

  @Override
  public List<PartialPath> getInvolvedStorageGroups(PartialPath pathPattern)
      throws MetadataException {
    return mtree.getBelongedStorageGroups(pathPattern);
  }
}
