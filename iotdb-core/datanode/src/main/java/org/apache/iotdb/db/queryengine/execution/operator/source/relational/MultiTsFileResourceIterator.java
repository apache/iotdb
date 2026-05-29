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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.FileLoaderUtils;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.file.metadata.AbstractAlignedTimeSeriesMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.LazyTsFileDeviceIterator;
import org.apache.tsfile.read.TsFileSequenceReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongConsumer;

public class MultiTsFileResourceIterator {

  private final String tableName;
  private final FragmentInstanceContext fragmentInstanceContext;
  private final SeriesScanOptions seriesScanOptions;
  private final Map<TsFileResource, TsFileResourceDeviceIterator> deviceIteratorMap =
      new HashMap<>();

  private IDeviceID currentDevice;

  public MultiTsFileResourceIterator(
      String tableName,
      List<TsFileResource> seqResources,
      List<TsFileResource> unseqResources,
      Map<TsFileResource, TsFileSequenceReader> resourceReaderMap,
      FragmentInstanceContext fragmentInstanceContext,
      SeriesScanOptions seriesScanOptions) {
    this.tableName = tableName;
    this.fragmentInstanceContext = fragmentInstanceContext;
    this.seriesScanOptions = seriesScanOptions;
    initDeviceIterators(seqResources, resourceReaderMap);
    initDeviceIterators(unseqResources, resourceReaderMap);
  }

  private void initDeviceIterators(
      List<TsFileResource> resources, Map<TsFileResource, TsFileSequenceReader> resourceReaderMap) {
    for (TsFileResource resource : resources) {
      try {
        TsFileSequenceReader reader = resourceReaderMap.get(resource);
        if (reader == null) {
          throw new IllegalArgumentException(
              "Missing external TsFile reader: " + resource.getTsFilePath());
        }
        deviceIteratorMap.put(resource, new TsFileResourceDeviceIterator(resource, reader));
      } catch (IOException e) {
        throw new RuntimeException(
            "Failed to create device iterator for external TsFile: " + resource.getTsFilePath(), e);
      }
    }
  }

  public boolean hasNextDevice() {
    for (TsFileResourceDeviceIterator iterator : deviceIteratorMap.values()) {
      if (iterator.hasNextDevice()
          || (iterator.getCurrentDevice() != null
              && !iterator.getCurrentDevice().equals(currentDevice))) {
        return true;
      }
    }
    return false;
  }

  public IDeviceID nextDevice() {
    IDeviceID nextDevice = null;
    List<TsFileResource> exhaustedResources = new ArrayList<>();
    for (Map.Entry<TsFileResource, TsFileResourceDeviceIterator> entry :
        deviceIteratorMap.entrySet()) {
      TsFileResource resource = entry.getKey();
      TsFileResourceDeviceIterator iterator = entry.getValue();
      if (iterator.getCurrentDevice() == null
          || iterator.getCurrentDevice().equals(currentDevice)) {
        if (iterator.hasNextDevice()) {
          if (iterator.nextDevice() == null) {
            exhaustedResources.add(resource);
            continue;
          }
        } else {
          exhaustedResources.add(resource);
          continue;
        }
      }
      if (nextDevice == null || nextDevice.compareTo(iterator.getCurrentDevice()) > 0) {
        nextDevice = iterator.getCurrentDevice();
      }
    }
    for (TsFileResource resource : exhaustedResources) {
      deviceIteratorMap.remove(resource);
    }
    currentDevice = nextDevice;
    return currentDevice;
  }

  public IDeviceID getCurrentDevice() {
    return currentDevice;
  }

  public AbstractAlignedTimeSeriesMetadata loadTimeSeriesMetadata(
      TsFileResource resource, AlignedFullPath alignedPath) throws IOException {
    TsFileResourceDeviceIterator iterator = deviceIteratorMap.get(resource);
    if (iterator == null
        || currentDevice == null
        || !currentDevice.equals(iterator.getCurrentDevice())) {
      return null;
    }
    return iterator.loadTimeSeriesMetadata(alignedPath);
  }

  public long[] getCurrentDeviceMeasurementNodeOffset(TsFileResource resource) {
    TsFileResourceDeviceIterator iterator = deviceIteratorMap.get(resource);
    if (iterator == null
        || currentDevice == null
        || !currentDevice.equals(iterator.getCurrentDevice())) {
      return null;
    }
    return iterator.getCurrentDeviceMeasurementNodeOffset();
  }

  private boolean isDeviceMatched(IDeviceID deviceID) {
    return tableName.equalsIgnoreCase(deviceID.getTableName());
  }

  private class TsFileResourceDeviceIterator {

    private final TsFileResource resource;
    private final LazyTsFileDeviceIterator deviceIterator;
    private IDeviceID currentDevice;

    private TsFileResourceDeviceIterator(TsFileResource resource, TsFileSequenceReader reader)
        throws IOException {
      this.resource = resource;
      LongConsumer ioSizeRecorder =
          fragmentInstanceContext.getQueryStatistics().getLoadTimeSeriesMetadataActualIOSize()
              ::addAndGet;
      this.deviceIterator = new LazyTsFileDeviceIterator(reader, tableName, ioSizeRecorder);
    }

    private boolean hasNextDevice() {
      return deviceIterator.hasNext();
    }

    private IDeviceID nextDevice() {
      while (deviceIterator.hasNext()) {
        IDeviceID nextDevice = deviceIterator.next();
        if (isDeviceMatched(nextDevice)) {
          currentDevice = nextDevice;
          return currentDevice;
        }
      }
      currentDevice = null;
      return null;
    }

    private IDeviceID getCurrentDevice() {
      return currentDevice;
    }

    private long[] getCurrentDeviceMeasurementNodeOffset() {
      return deviceIterator.getCurrentDeviceMeasurementNodeOffset();
    }

    private AbstractAlignedTimeSeriesMetadata loadTimeSeriesMetadata(AlignedFullPath alignedPath)
        throws IOException {
      // TODO: Pass getCurrentDeviceMeasurementNodeOffset() to FileLoaderUtils after this branch
      // supports offset-based metadata loading.
      return FileLoaderUtils.loadAlignedTimeSeriesMetadata(
          resource,
          alignedPath,
          fragmentInstanceContext,
          seriesScanOptions.getGlobalTimeFilter(),
          resource.isSeq(),
          fragmentInstanceContext.isIgnoreAllNullRows());
    }
  }
}
