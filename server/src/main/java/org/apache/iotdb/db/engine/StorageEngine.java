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
package org.apache.iotdb.db.engine;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.exception.ShutdownException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StorageEngine implements IService {
  private static final Logger logger = LoggerFactory.getLogger(StorageEngine.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private StorageEngine() {}

  public static StorageEngine getInstance() {
    return InstanceHolder.INSTANCE;
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public void shutdown(long milliseconds) throws ShutdownException {}

  @Override
  public ServiceType getID() {
    return ServiceType.STORAGE_ENGINE_SERVICE;
  }

  /**
   * This method is for insert and query or sth like them, this may get a virtual database
   *
   * @param path device path
   * @return database processor
   */
  public DataRegion getProcessor(PartialPath path) throws StorageEngineException {
    try {
      IStorageGroupMNode storageGroupMNode = IoTDB.schemaProcessor.getStorageGroupNodeByPath(path);
      return getStorageGroupProcessorByPath(path, storageGroupMNode);
    } catch (DataRegionException | MetadataException e) {
      throw new StorageEngineException(e);
    }
  }

  /**
   * get database processor by device path
   *
   * @param devicePath path of the device
   * @param storageGroupMNode mnode of the storage group, we need synchronize this to avoid
   *     modification in mtree
   * @return found or new storage group processor
   */
  private DataRegion getStorageGroupProcessorByPath(
      PartialPath devicePath, IStorageGroupMNode storageGroupMNode) throws DataRegionException {
    return null;
  }

  /** get all merge lock of the storage group processor related to the query */
  public Pair<List<DataRegion>, Map<DataRegion, List<PartialPath>>> mergeLock(
      List<PartialPath> pathList) throws StorageEngineException {
    Map<DataRegion, List<PartialPath>> map = new HashMap<>();
    for (PartialPath path : pathList) {
      map.computeIfAbsent(getProcessor(path.getDevicePath()), key -> new ArrayList<>()).add(path);
    }
    List<DataRegion> list =
        map.keySet().stream()
            .sorted(Comparator.comparing(DataRegion::getDataRegionId))
            .collect(Collectors.toList());
    list.forEach(DataRegion::readLock);

    return new Pair<>(list, map);
  }

  /** unlock all merge lock of the storage group processor related to the query */
  public void mergeUnLock(List<DataRegion> list) {
    list.forEach(DataRegion::readUnlock);
  }

  public String getStorageGroupPath(PartialPath selectedPath) {
    return null;
  }

  static class InstanceHolder {

    private static final StorageEngine INSTANCE = new StorageEngine();

    private InstanceHolder() {
      // forbidding instantiation
    }
  }
}
