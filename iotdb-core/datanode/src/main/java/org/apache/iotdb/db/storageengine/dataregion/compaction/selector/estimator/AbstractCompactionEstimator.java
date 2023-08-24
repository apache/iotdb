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

package org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.flush.CompressionRatio;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Estimate the memory cost of one compaction task with specific source files based on its
 * corresponding implementation.
 */
public abstract class AbstractCompactionEstimator implements Closeable {

  protected Map<TsFileResource, TsFileSequenceReader> fileReaderCache = new HashMap<>();

  protected IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  protected List<TsFileResource> seqResources;
  protected List<TsFileResource> unseqResources;

  protected long compressionRatio = (long) CompressionRatio.getInstance().getRatio() + 1;

  protected abstract long calculatingMetadataMemoryCost(CompactionTaskInfo taskInfo);

  protected abstract long calculatingDataMemoryCost(CompactionTaskInfo taskInfo) throws IOException;

  /**
   * Construct a new or get an existing TsFileSequenceReader of a TsFile.
   *
   * @throws IOException if io errors occurred
   */
  protected TsFileSequenceReader getFileReader(TsFileResource tsFileResource) throws IOException {
    TsFileSequenceReader reader = fileReaderCache.get(tsFileResource);
    if (reader == null) {
      reader = new TsFileSequenceReader(tsFileResource.getTsFilePath(), true, false);
      fileReaderCache.put(tsFileResource, reader);
    }
    return reader;
  }

  protected CompactionTaskInfo calculatingCompactionTaskInfo(List<TsFileResource> resources)
      throws IOException {
    List<FileInfo> fileInfoList = new ArrayList<>();
    for (TsFileResource resource : resources) {
      resource.deserialize();
      TsFileSequenceReader reader = getFileReader(resource);
      FileInfo fileInfo = CompactionEstimateUtils.getSeriesAndDeviceChunkNum(reader);
      fileInfoList.add(fileInfo);
    }
    return new CompactionTaskInfo(resources, fileInfoList);
  }

  protected int calculatingMaxOverlapFileNumInSubCompactionTask(List<TsFileResource> resources)
      throws IOException {
    Set<String> devices = new HashSet<>();
    for (TsFileResource resource : resources) {
      resource.deserialize();
      devices.addAll(resource.getDevices());
    }
    int maxOverlapFileNumInSubCompactionTask = 1;
    for (String device : devices) {
      List<TsFileResource> resourcesContainsCurrentDevice =
          resources.stream()
              .filter(resource -> !resource.definitelyNotContains(device))
              .sorted(Comparator.comparingLong(resource -> resource.getStartTime(device)))
              .collect(Collectors.toList());
      long maxEndTimeOfCurrentDevice = Long.MIN_VALUE;
      int overlapFileNumOfCurrentDevice = 0;
      for (TsFileResource resource : resourcesContainsCurrentDevice) {
        long deviceStartTimeInCurrentFile = resource.getStartTime(device);
        long deviceEndTimeInCurrentFile = resource.getEndTime(device);
        if (deviceStartTimeInCurrentFile <= maxEndTimeOfCurrentDevice) {
          // has overlap, update max end time
          maxEndTimeOfCurrentDevice =
              Math.max(maxEndTimeOfCurrentDevice, deviceEndTimeInCurrentFile);
          overlapFileNumOfCurrentDevice++;
          maxOverlapFileNumInSubCompactionTask =
              Math.max(maxOverlapFileNumInSubCompactionTask, overlapFileNumOfCurrentDevice);
        } else {
          // reset max end time and overlap file num of current device
          maxEndTimeOfCurrentDevice = deviceEndTimeInCurrentFile;
          overlapFileNumOfCurrentDevice = 1;
        }
      }
      // already reach the max value
      if (maxOverlapFileNumInSubCompactionTask == resources.size()) {
        return maxOverlapFileNumInSubCompactionTask;
      }
    }
    return maxOverlapFileNumInSubCompactionTask;
  }

  public void close() throws IOException {
    for (TsFileSequenceReader reader : fileReaderCache.values()) {
      reader.close();
    }
    fileReaderCache.clear();
  }
}
