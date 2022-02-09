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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.utils.FilePathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

/** This class manages one id table for each logical storage group */
public class IDTableManager {

  /** logger */
  Logger logger = LoggerFactory.getLogger(IDTableManager.class);

  /** storage group path -> id table */
  HashMap<String, IDTable> idTableMap;

  /** system dir */
  private final String systemDir =
      FilePathUtils.regularizePath(IoTDBDescriptor.getInstance().getConfig().getSystemDir())
          + "storage_groups";

  // region IDManager Singleton
  private static class IDManagerHolder {

    private IDManagerHolder() {
      // allowed to do nothing
    }

    private static final IDTableManager INSTANCE = new IDTableManager();
  }

  /**
   * get instance
   *
   * @return instance of the factory
   */
  public static IDTableManager getInstance() {
    return IDManagerHolder.INSTANCE;
  }

  private IDTableManager() {
    idTableMap = new HashMap<>();
  }
  // endregion

  /**
   * get id table by device path
   *
   * @param devicePath device path
   * @return id table belongs to path's storage group
   */
  public synchronized IDTable getIDTable(PartialPath devicePath) {
    try {
      IStorageGroupMNode storageGroupMNode =
          IoTDB.metaManager.getStorageGroupNodeByPath(devicePath);
      return idTableMap.computeIfAbsent(
          storageGroupMNode.getFullPath(),
          storageGroupPath ->
              new IDTableHashmapImpl(
                  SystemFileFactory.INSTANCE.getFile(
                      systemDir + File.separator + storageGroupPath)));
    } catch (MetadataException e) {
      logger.error("get id table failed, path is: " + devicePath + ". caused by: " + e);
    }

    return null;
  }

  /** clear id table map */
  public void clear() throws IOException {
    for (IDTable idTable : idTableMap.values()) {
      idTable.clear();
    }

    idTableMap.clear();
  }
}
