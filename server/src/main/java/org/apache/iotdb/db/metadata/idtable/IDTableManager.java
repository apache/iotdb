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
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

/** This class manages one id table for each logical database */
public class IDTableManager {

  /** logger */
  Logger logger = LoggerFactory.getLogger(IDTableManager.class);

  /** database path -> id table */
  HashMap<String, IDTable> idTableMap;

  /** system dir */
  private final String systemDir =
      FilePathUtils.regularizePath(IoTDBDescriptor.getInstance().getConfig().getSystemDir())
          + "databases";

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
   * @return id table belongs to path's database
   */
  public synchronized IDTable getIDTable(PartialPath devicePath) {
    //    try {
    //      return idTableMap.computeIfAbsent(
    //
    // LocalSchemaProcessor.getInstance().getStorageGroupNodeByPath(devicePath).getFullPath(),
    //          storageGroupPath ->
    //              new IDTableHashmapImpl(
    //                  SystemFileFactory.INSTANCE.getFile(
    //                      systemDir + File.separator + storageGroupPath)));
    //    } catch (MetadataException e) {
    //      logger.error("get id table failed, path is: " + devicePath + ". caused by: " + e);
    //    }
    //
    //    return null;
    throw new UnsupportedOperationException();
  }

  /**
   * get id table by database path
   *
   * @param sgPath database path
   * @return id table belongs to path's database
   */
  public synchronized IDTable getIDTableDirectly(String sgPath) {
    return idTableMap.computeIfAbsent(
        sgPath,
        storageGroupPath ->
            new IDTableHashmapImpl(
                SystemFileFactory.INSTANCE.getFile(systemDir + File.separator + storageGroupPath)));
  }

  /**
   * get schema from device and measurements
   *
   * @param deviceName device name of the time series
   * @param measurementName measurement name of the time series
   * @return schema entry of the time series
   */
  public synchronized IMeasurementSchema getSeriesSchema(String deviceName, String measurementName)
      throws MetadataException {
    for (IDTable idTable : idTableMap.values()) {
      IMeasurementSchema measurementSchema = idTable.getSeriesSchema(deviceName, measurementName);
      if (measurementSchema != null) {
        return measurementSchema;
      }
    }

    throw new PathNotExistException(new PartialPath(deviceName, measurementName).toString());
  }

  /** clear id table map */
  public void clear() throws IOException {
    for (IDTable idTable : idTableMap.values()) {
      idTable.clear();
    }

    idTableMap.clear();
  }
}
