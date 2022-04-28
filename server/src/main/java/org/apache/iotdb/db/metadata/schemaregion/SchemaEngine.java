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

package org.apache.iotdb.db.metadata.schemaregion;

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.storagegroup.IStorageGroupSchemaManager;
import org.apache.iotdb.db.metadata.storagegroup.StorageGroupSchemaManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// manage all the schemaRegion in this dataNode
public class SchemaEngine {

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final IStorageGroupSchemaManager localStorageGroupSchemaManager =
      StorageGroupSchemaManager.getInstance();

  private Map<SchemaRegionId, ISchemaRegion> schemaRegionMap;
  private SchemaEngineMode schemaRegionStoredMode;
  private static final Logger logger = LoggerFactory.getLogger(SchemaEngine.class);
  private static final String RSCHEMA_REGION_CLASS_NAME =
      "org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaRegion";
  private static final String RSCHEMA_CONF_LOADER_CLASS_NAME =
      "org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConfLoader";
  private static final String LIB_PATH = ".." + File.separator + "lib" + File.separator;

  private static class SchemaEngineManagerHolder {

    private static final SchemaEngine INSTANCE = new SchemaEngine();

    private SchemaEngineManagerHolder() {}
  }

  private SchemaEngine() {}

  public static SchemaEngine getInstance() {
    return SchemaEngineManagerHolder.INSTANCE;
  }

  public Map<PartialPath, List<SchemaRegionId>> init() throws MetadataException {
    schemaRegionMap = new ConcurrentHashMap<>();
    schemaRegionStoredMode = SchemaEngineMode.valueOf(config.getSchemaEngineMode());
    logger.info("used schema engine mode: {}.", schemaRegionStoredMode);

    return initSchemaRegion();
  }

  /**
   * Scan the storage group and schema region directories to recover schema regions and return the
   * collected local schema partition info for localSchemaPartitionTable recovery.
   */
  private Map<PartialPath, List<SchemaRegionId>> initSchemaRegion() throws MetadataException {
    Map<PartialPath, List<SchemaRegionId>> partitionTable = new HashMap<>();
    for (PartialPath storageGroup : localStorageGroupSchemaManager.getAllStorageGroupPaths()) {
      List<SchemaRegionId> schemaRegionIdList = new ArrayList<>();
      partitionTable.put(storageGroup, schemaRegionIdList);

      File sgDir = new File(config.getSchemaDir(), storageGroup.getFullPath());

      if (!sgDir.exists()) {
        continue;
      }

      File[] schemaRegionDirs = sgDir.listFiles();
      if (schemaRegionDirs == null) {
        continue;
      }

      for (File schemaRegionDir : schemaRegionDirs) {
        SchemaRegionId schemaRegionId;
        try {
          schemaRegionId = new SchemaRegionId(Integer.parseInt(schemaRegionDir.getName()));
        } catch (NumberFormatException e) {
          // the dir/file is not schemaRegionDir, ignore this.
          continue;
        }
        createSchemaRegion(storageGroup, schemaRegionId);
        schemaRegionIdList.add(schemaRegionId);
      }
    }
    return partitionTable;
  }

  public void forceMlog() {
    if (schemaRegionMap != null) {
      for (ISchemaRegion schemaRegion : schemaRegionMap.values()) {
        schemaRegion.forceMlog();
      }
    }
  }

  public void clear() {
    if (schemaRegionMap != null) {
      for (ISchemaRegion schemaRegion : schemaRegionMap.values()) {
        schemaRegion.clear();
      }
      schemaRegionMap.clear();
      schemaRegionMap = null;
    }
  }

  public ISchemaRegion getSchemaRegion(SchemaRegionId regionId) {
    return schemaRegionMap.get(regionId);
  }

  public Collection<ISchemaRegion> getAllSchemaRegions() {
    return schemaRegionMap.values();
  }

  public synchronized void createSchemaRegion(
      PartialPath storageGroup, SchemaRegionId schemaRegionId) throws MetadataException {
    ISchemaRegion schemaRegion = schemaRegionMap.get(schemaRegionId);
    if (schemaRegion != null) {
      return;
    }
    localStorageGroupSchemaManager.ensureStorageGroup(storageGroup);
    IStorageGroupMNode storageGroupMNode =
        localStorageGroupSchemaManager.getStorageGroupNodeByStorageGroupPath(storageGroup);
    switch (schemaRegionStoredMode) {
      case Memory:
        schemaRegion = new SchemaRegionMemoryImpl(storageGroup, schemaRegionId, storageGroupMNode);
        break;
      case Schema_File:
        schemaRegion =
            new SchemaRegionSchemaFileImpl(storageGroup, schemaRegionId, storageGroupMNode);
        break;
      case Rocksdb_based:
        loadRSchemaRegion(storageGroup, schemaRegionId, storageGroupMNode);
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(
                "This mode [%s] is not supported. Please check and modify it.",
                schemaRegionStoredMode));
    }
    schemaRegionMap.put(schemaRegionId, schemaRegion);
  }

  public void deleteSchemaRegion(SchemaRegionId schemaRegionId) throws MetadataException {
    schemaRegionMap.get(schemaRegionId).deleteSchemaRegion();
    schemaRegionMap.remove(schemaRegionId);
  }

  private ISchemaRegion loadRSchemaRegion(
      PartialPath storageGroup, SchemaRegionId schemaRegionId, IStorageGroupMNode node) {
    ISchemaRegion region = null;
    try {
      loadRSchemaRegionJar();
      Class<?> classForRSchemaRegion = Class.forName(RSCHEMA_REGION_CLASS_NAME);
      Class<?> classForRSchemaConfLoader = Class.forName(RSCHEMA_CONF_LOADER_CLASS_NAME);
      Constructor<?> constructor =
          classForRSchemaRegion.getConstructor(
              PartialPath.class,
              SchemaRegionId.class,
              IStorageGroupMNode.class,
              classForRSchemaConfLoader);
      region =
          (ISchemaRegion)
              constructor.newInstance(
                  storageGroup, schemaRegionId, node, classForRSchemaConfLoader.newInstance());
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | InvocationTargetException
        | InstantiationException
        | IllegalAccessException
        | MalformedURLException e) {
      logger.error("Cannot initialize RSchemaRegion", e);
    }
    return region;
  }

  private void loadRSchemaRegionJar()
      throws NoSuchMethodException, MalformedURLException, InvocationTargetException,
          IllegalAccessException {
    File[] jars = new File(LIB_PATH).listFiles();
    for (File jar : jars) {
      if (jar.getName().contains("rocksdb")) {
        // 从URLClassLoader类中获取类所在文件夹的方法，jar也可以认为是一个文件夹
        Method method = null;
        try {
          method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
        } catch (NoSuchMethodException | SecurityException e) {
          throw e;
        }
        // 获取方法的访问权限以便写回
        boolean accessible = method.isAccessible();
        try {
          method.setAccessible(true);
          // 获取系统类加载器
          URLClassLoader classLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
          URL url = jar.toURI().toURL();
          method.invoke(classLoader, url);
        } catch (Exception e) {
          throw e;
        } finally {
          method.setAccessible(accessible);
        }
      }
    }
  }
  //
  //  private RSchemaConfLoader loadRocksdbConfFile() {
  //    if (rSchemaConfLoader == null) {
  //      rSchemaConfLoader = new RSchemaConfLoader();
  //    }
  //    return rSchemaConfLoader;
  //  }
}
