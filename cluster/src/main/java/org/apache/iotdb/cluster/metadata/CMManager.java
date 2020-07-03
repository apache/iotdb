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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MeasurementMeta;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.service.IoTDB;
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

  // currently, if a key is not existed in the mRemoteMetaCache, an IOException will be thrown
  private ReentrantReadWriteLock cacheLock = new ReentrantReadWriteLock();;
  private LRUCache<String, MeasurementMeta> mRemoteMetaCache;
  private MetaPuller metaPuller;

  protected CMManager() {
    super();
    metaPuller = MetaPuller.getInstance();
    int remoteCacheSize = config.getmRemoteSchemaCacheSize();
    mRemoteMetaCache = new LRUCache<String, MeasurementMeta>(remoteCacheSize) {
      @Override
      protected MeasurementMeta loadObjectByKey(String key) throws IOException {
        throw new IOException(key + " not found!");
      }

      @Override
      public synchronized void removeItem(String key) {
        cache.keySet().removeIf(s -> s.startsWith(key));
      }
    };
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
    try {
      cacheLock.readLock().lock();
      MeasurementMeta measurementMeta = mRemoteMetaCache.get(path);
      return measurementMeta.getMeasurementSchema().getType();
    } catch (IOException e) {
      //do nothing
    } finally {
      cacheLock.readLock().unlock();
    }
    return super.getSeriesType(path);
  }

  @Override
  public MeasurementSchema[] getSchemas(String deviceId, String[] measurements) throws MetadataException {
    try {
      return super.getSchemas(deviceId, measurements);
    } catch (MetadataException e) {
      // some measurements not exist in local
      // try cache
      MeasurementSchema[] measurementSchemas = new MeasurementSchema[measurements.length];
      boolean allSeriesExists = true;
      cacheLock.readLock().lock();
      for (int i = 0; i < measurements.length; i++) {
        try {
          MeasurementMeta measurementMeta = mRemoteMetaCache.get(deviceId + measurements[i]);
          measurementSchemas[i] = measurementMeta.getMeasurementSchema();
        } catch (IOException ex) {
          // not all cached, pull from remote
          allSeriesExists = false;
          break;
        }
      }
      cacheLock.readLock().unlock();
      if (allSeriesExists) {
        return measurementSchemas;
      }

      pullSeriesSchemas(deviceId, measurements);

      // try again
      boolean allExist = true;
      cacheLock.readLock().lock();
      for (int i = 0; i < measurements.length; i++) {
        try {
          MeasurementMeta measurementMeta = mRemoteMetaCache.get(deviceId + measurements[i]);
          measurementSchemas[i] = measurementMeta.getMeasurementSchema();
        } catch (IOException ex) {
          allExist = false;
          break;
        }
      }
      cacheLock.readLock().unlock();
      if (!allExist) {
        throw new MetadataException(deviceId + " has some mesurements not found");
      }
      return measurementSchemas;
    }
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
      measurementMeta.updateCachedLast(timeValuePair, highPriorityUpdate, latestFlushedTime);
    } catch (IOException e) {
      // not found
    } finally {
      cacheLock.writeLock().unlock();
    }
    // maybe local also has the timeseries
    super.updateLastCache(seriesPath, timeValuePair, highPriorityUpdate, latestFlushedTime);
  }

  @Override
  public TimeValuePair getLastCache(String seriesPath) {
    try {
      MeasurementMeta measurementMeta = mRemoteMetaCache.get(seriesPath);
      return measurementMeta.getTimeValuePair();
    } catch (IOException e) {
      // do nothing
    }
    return super.getLastCache(seriesPath);
  }

  @Override
  public MeasurementSchema[] getSeriesSchemasAndReadLockDevice(String deviceId, String[] measurementList, InsertPlan plan) throws MetadataException {
    //TODO cluster also need to lock device node
    boolean allSeriesExists = true;
    MeasurementSchema[] measurementSchemas = new MeasurementSchema[measurementList.length];
    for (int i = 0; i < measurementList.length; i++) {
      MeasurementMeta measurementMeta = null;
      try {
        measurementMeta = mRemoteMetaCache.get(deviceId + measurementList[i]);
        measurementSchemas[i] = measurementMeta.getMeasurementSchema();
      } catch (IOException e) {
        // ignore
        allSeriesExists = false;
        break;
      }
    }
    if (allSeriesExists) {
      return measurementSchemas;
    }
    return super.getSeriesSchemasAndReadLockDevice(deviceId, measurementList, plan);
  }


}
