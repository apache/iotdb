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

package org.apache.iotdb.cluster.metadata;

import java.util.Collections;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MeasurementMeta;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.tsfile.common.cache.LRUCache;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CMManager extends MManager {

  private static final Logger logger = LoggerFactory.getLogger(CMManager.class);

  private ReentrantReadWriteLock cacheLock = new ReentrantReadWriteLock();
  // only cache the series who is writing, we need not to cache series who is reading
  // because the read is slow, so pull from remote is little cost comparing to the disk io
  private RemoteMetaCache mRemoteMetaCache;
  private MetaPuller metaPuller;

  protected CMManager() {
    super();
    metaPuller = MetaPuller.getInstance();
    int remoteCacheSize = config.getmRemoteSchemaCacheSize();
    mRemoteMetaCache = new RemoteMetaCache(remoteCacheSize);
  }

  private static class MManagerHolder {

    private MManagerHolder() {
      // allowed to do nothing
    }

    private static final CMManager INSTANCE = new CMManager();
  }

  /**
   * we should not use this function in other place, but only in IoTDB class
   * @return
   */
  public static MManager getInstance() {
    return CMManager.MManagerHolder.INSTANCE;
  }

  @Override
  public String deleteTimeseries(String prefixPath) throws MetadataException {
    cacheLock.writeLock().lock();
    mRemoteMetaCache.removeItem(prefixPath);
    cacheLock.writeLock().unlock();
    return super.deleteTimeseries(prefixPath);
  }

  @Override
  public void deleteStorageGroups(List<String> storageGroups) throws MetadataException {
    cacheLock.writeLock().lock();
    for (String storageGroup : storageGroups) {
      mRemoteMetaCache.removeItem(storageGroup);
    }
    cacheLock.writeLock().unlock();
    super.deleteStorageGroups(storageGroups);
  }

  @Override
  public TSDataType getSeriesType(String path) throws MetadataException {
    // try remote cache first
    try {
      cacheLock.readLock().lock();
      MeasurementMeta measurementMeta = mRemoteMetaCache.get(path);
      if (measurementMeta != null) {
        return measurementMeta.getMeasurementSchema().getType();
      }
    } finally {
      cacheLock.readLock().unlock();
    }

    // try local MTree
    TSDataType seriesType;
    try {
      seriesType = super.getSeriesType(path);
    } catch (PathNotExistException e) {
      // pull from remote node
      List<MeasurementSchema> schemas = metaPuller
          .pullTimeSeriesSchemas(Collections.singletonList(path));
      if (!schemas.isEmpty()) {
        cacheMeta(path, new MeasurementMeta(schemas.get(0)));
        return schemas.get(0).getType();
      } else {
        throw e;
      }
    }
    return seriesType;
  }

  /**
   *  the org.apache.iotdb.db.writelog.recover.logReplayer will call this to get schema after restart
   *  we should retry to get schema util we get the schema
   * @param deviceId
   * @param measurements
   * @return
   * @throws MetadataException
   */
  @Override
  public MeasurementSchema[] getSchemas(String deviceId, String[] measurements) throws MetadataException {
    try {
      return super.getSchemas(deviceId, measurements);
    } catch (MetadataException e) {
      // some measurements not exist in local
      // try cache
      MeasurementSchema[] measurementSchemas = new MeasurementSchema[measurements.length];
      int failedMeasurementIndex = getSchemasLocally(deviceId, measurements, measurementSchemas);
      if (failedMeasurementIndex == -1) {
        return measurementSchemas;
      }

      // will retry util get schema
      pullSeriesSchemas(deviceId, measurements);

      // try again
      failedMeasurementIndex = getSchemasLocally(deviceId, measurements, measurementSchemas);
      if (failedMeasurementIndex != -1) {
        throw new MetadataException(deviceId + IoTDBConstant.PATH_SEPARATOR
          + measurements[failedMeasurementIndex] + " is not found");
      }
      return measurementSchemas;
    }
  }

  /**
   *
   * @return -1 if all schemas are found, or the first index of the non-exist schema
   */
  private int getSchemasLocally(String deviceId, String[] measurements, MeasurementSchema[] measurementSchemas) {
    int failedMeasurementIndex = -1;
    cacheLock.readLock().lock();
    try {
      for (int i = 0; i < measurements.length && failedMeasurementIndex == -1; i++) {
        MeasurementMeta measurementMeta = mRemoteMetaCache.get(deviceId + IoTDBConstant.PATH_SEPARATOR  + measurements[i]);
        if (measurementMeta == null) {
          failedMeasurementIndex = i;
        } else {
          measurementSchemas[i] = measurementMeta.getMeasurementSchema();
        }
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return failedMeasurementIndex;
  }

  private void pullSeriesSchemas(String deviceId, String[] measurementList)
    throws MetadataException {
    List<String> schemasToPull = new ArrayList<>();
    for (String s : measurementList) {
      schemasToPull.add(deviceId + IoTDBConstant.PATH_SEPARATOR + s);
    }
    List<MeasurementSchema> schemas = metaPuller.pullTimeSeriesSchemas(schemasToPull);
    for (MeasurementSchema schema : schemas) {
      cacheMeta(deviceId + IoTDBConstant.PATH_SEPARATOR + schema.getMeasurementId(), new MeasurementMeta(schema));
    }
    logger.debug("Pulled {}/{} schemas from remote", schemas.size(), measurementList.length);
  }

  @Override
  public void cacheMeta(String seriesPath, MeasurementMeta meta) {
    cacheLock.writeLock().lock();
    mRemoteMetaCache.put(seriesPath, meta);
    cacheLock.writeLock().unlock();
  }

  @Override
  public void updateLastCache(String seriesPath, TimeValuePair timeValuePair, boolean highPriorityUpdate, Long latestFlushedTime) {
    cacheLock.writeLock().lock();
    try {
      MeasurementMeta measurementMeta = mRemoteMetaCache.get(seriesPath);
      if (measurementMeta != null) {
        measurementMeta.updateCachedLast(timeValuePair, highPriorityUpdate, latestFlushedTime);
      }
    } finally {
      cacheLock.writeLock().unlock();
    }
    // maybe local also has the timeseries
    super.updateLastCache(seriesPath, timeValuePair, highPriorityUpdate, latestFlushedTime);
  }

  @Override
  public TimeValuePair getLastCache(String seriesPath) {
    MeasurementMeta measurementMeta = mRemoteMetaCache.get(seriesPath);
    if (measurementMeta != null) {
      return measurementMeta.getTimeValuePair();
    }

    return super.getLastCache(seriesPath);
  }

  @Override
  public MeasurementSchema[] getSeriesSchemasAndReadLockDevice(String deviceId, String[] measurementList, InsertPlan plan) throws MetadataException {
    MeasurementSchema[] measurementSchemas = new MeasurementSchema[measurementList.length];
    int nonExistSchemaIndex = getSchemasLocally(deviceId, measurementList, measurementSchemas);
    if (nonExistSchemaIndex == -1) {
      return measurementSchemas;
    }
    // auto-create schema in IoTDBConfig is always disabled in the cluster version, and we have
    // another config in ClusterConfig to do this
    return super.getSeriesSchemasAndReadLockDevice(deviceId, measurementList, plan);
  }

  @Override
  public MeasurementSchema getSeriesSchema(String device, String measurement) throws MetadataException {
    try {
      MeasurementSchema measurementSchema = super.getSeriesSchema(device, measurement);
      if (measurementSchema != null) {
        return measurementSchema;
      }
    } catch (PathNotExistException e) {
      // not found in local
    }

    // try cache
    cacheLock.readLock().lock();
    try {
      MeasurementMeta measurementMeta = mRemoteMetaCache.get(device + IoTDBConstant.PATH_SEPARATOR + measurement);
      if (measurementMeta != null) {
        return measurementMeta.getMeasurementSchema();
      }
    } finally {
      cacheLock.readLock().unlock();
    }

    // pull from remote
    pullSeriesSchemas(device, new String[]{measurement});

    // try again
    cacheLock.readLock().lock();
    try {
      MeasurementMeta measurementMeta = mRemoteMetaCache.get(device + IoTDBConstant.PATH_SEPARATOR + measurement);
      if (measurementMeta != null) {
        return measurementMeta.getMeasurementSchema();
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return super.getSeriesSchema(device, measurement);
  }

  private static class RemoteMetaCache extends LRUCache<String, MeasurementMeta> {

    public RemoteMetaCache(int cacheSize) {
      super(cacheSize);
    }

    @Override
    protected MeasurementMeta loadObjectByKey(String key) {
      return null;
    }

    @Override
    public synchronized void removeItem(String key) {
      cache.keySet().removeIf(s -> s.startsWith(key));
    }

    @Override
    public synchronized MeasurementMeta get(String key) {
      try {
        return super.get(key);
      } catch (IOException e) {
        // not happening
        return null;
      }
    }
  }
}
