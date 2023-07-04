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

package org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * CrossSpaceCompactionResource manages files and caches of readers to avoid unnecessary object
 * creations and file openings.
 */
public class CrossSpaceCompactionCandidate {
  private List<TsFileResourceCandidate> seqFiles;
  private List<TsFileResourceCandidate> unseqFiles;

  private int nextUnseqFileIndex;
  private CrossCompactionTaskResourceSplit nextSplit;
  private long ttlLowerBound = Long.MIN_VALUE;

  public CrossSpaceCompactionCandidate(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    init(seqFiles, unseqFiles);
  }

  public CrossSpaceCompactionCandidate(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles, long ttlLowerBound) {
    this.ttlLowerBound = ttlLowerBound;
    init(seqFiles, unseqFiles);
  }

  private void init(List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    this.seqFiles = copySeqResource(seqFiles);
    // it is necessary that unseqFiles are all available
    this.unseqFiles = filterUnseqResource(unseqFiles);
    this.nextUnseqFileIndex = 0;
  }

  public boolean hasNextSplit() throws IOException {
    if (nextUnseqFileIndex >= unseqFiles.size()) {
      return false;
    }
    return prepareNextSplit();
  }

  public CrossCompactionTaskResourceSplit nextSplit() {
    return nextSplit;
  }

  @SuppressWarnings({"squid:S3776", "squid:S135"})
  private boolean prepareNextSplit() throws IOException {
    boolean nextUnseqFileHasOverlap = false;
    TsFileResourceCandidate unseqFile = unseqFiles.get(nextUnseqFileIndex);
    List<TsFileResourceCandidate> ret = new ArrayList<>();

    // The startTime and endTime of each device are different in one TsFile. So we need to do the
    // check one by one. And we cannot skip any device in the unseq file because it may lead to
    // omission of target seq file
    if (!unseqFile.hasDetailedDeviceInfo()) {
      // unseq file resource has been deleted due to TTL and cannot upgrade to DEVICE_TIME_INDEX
      return false;
    }
    for (DeviceInfo unseqDeviceInfo : unseqFile.getDevices()) {
      for (TsFileResourceCandidate seqFile : seqFiles) {
        // If the seqFile may need to be selected but its invalid, the selection should be
        // terminated.
        if ((!seqFile.isValidCandidate || !seqFile.hasDetailedDeviceInfo())
            && seqFile.mayHasOverlapWithUnseqFile(unseqDeviceInfo)) {
          return false;
        }
        if (!seqFile.containsDevice(unseqDeviceInfo.deviceId)) {
          continue;
        }
        DeviceInfo seqDeviceInfo = seqFile.getDeviceInfoById(unseqDeviceInfo.deviceId);

        // If the unsealed file is unclosed, the file should not be selected only when its startTime
        // is larger than endTime of unseqFile. Or, the selection should be terminated.
        if (seqFile.unsealed() && unseqDeviceInfo.endTime >= seqDeviceInfo.startTime) {
          return false;
        }
        if (unseqDeviceInfo.endTime <= seqDeviceInfo.endTime) {
          // When scanning the target seqFiles for unseqFile, we traverse them one by one no matter
          // whether it is selected or not. But we only add the unselected seqFiles to next split to
          // avoid duplication selection
          if (!seqFile.selected) {
            ret.add(seqFile);
            seqFile.markAsSelected();
          }
          nextUnseqFileHasOverlap = true;
          // if this condition is satisfied, all subsequent seq files is unnecessary to check
          break;
        } else if (unseqDeviceInfo.startTime <= seqDeviceInfo.endTime) {
          if (!seqFile.selected) {
            ret.add(seqFile);
            seqFile.markAsSelected();
          }
          nextUnseqFileHasOverlap = true;
        }
      }
    }
    // mark candidates in next split as selected even though it may not be added to the final
    // TaskResource
    unseqFile.markAsSelected();
    nextSplit = new CrossCompactionTaskResourceSplit(unseqFile, ret, nextUnseqFileHasOverlap);
    nextUnseqFileIndex++;
    return true;
  }

  private List<TsFileResourceCandidate> copySeqResource(List<TsFileResource> seqFiles) {
    List<TsFileResourceCandidate> ret = new ArrayList<>();
    for (TsFileResource resource : seqFiles) {
      ret.add(new TsFileResourceCandidate(resource));
    }
    return ret;
  }

  /**
   * Filter the unseq files into the compaction. Unseq files should be not deleted or over ttl. To
   * ensure that the compaction is correct, return as soon as it encounters the file being compacted
   * or compaction candidate. Therefore, a cross space compaction can only be performed serially
   * under a time partition in a VSG.
   */
  private List<TsFileResourceCandidate> filterUnseqResource(List<TsFileResource> unseqResources) {
    List<TsFileResourceCandidate> ret = new ArrayList<>();
    for (TsFileResource resource : unseqResources) {
      if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
        break;
      } else if (resource.stillLives(ttlLowerBound)) {
        ret.add(new TsFileResourceCandidate(resource));
      }
    }
    return ret;
  }

  public List<TsFileResource> getSeqFiles() {
    return seqFiles.stream()
        .map(tsFileResourceCandidate -> tsFileResourceCandidate.resource)
        .collect(Collectors.toList());
  }

  public List<TsFileResourceCandidate> getSeqFileCandidates() {
    return seqFiles;
  }

  public List<TsFileResourceCandidate> getUnseqFileCandidates() {
    return unseqFiles;
  }

  public List<TsFileResource> getUnseqFiles() {
    return unseqFiles.stream()
        .map(tsFileResourceCandidate -> tsFileResourceCandidate.resource)
        .collect(Collectors.toList());
  }

  public static class CrossCompactionTaskResourceSplit {
    @SuppressWarnings("squid:S1104")
    public TsFileResourceCandidate unseqFile;

    @SuppressWarnings("squid:S1104")
    public List<TsFileResourceCandidate> seqFiles;

    @SuppressWarnings("squid:S1104")
    public boolean hasOverlap;

    public CrossCompactionTaskResourceSplit(
        TsFileResourceCandidate unseqFile,
        List<TsFileResourceCandidate> seqFiles,
        boolean hasOverlap) {
      this.unseqFile = unseqFile;
      this.seqFiles = seqFiles;
      this.hasOverlap = hasOverlap;
    }
  }

  public static class TsFileResourceCandidate {
    @SuppressWarnings("squid:S1104")
    public TsFileResource resource;

    @SuppressWarnings("squid:S1104")
    public boolean selected;

    @SuppressWarnings("squid:S1104")
    public boolean isValidCandidate;

    private Map<String, DeviceInfo> deviceInfoMap;

    private boolean hasDetailedDeviceInfo;

    protected TsFileResourceCandidate(TsFileResource tsFileResource) {
      this.resource = tsFileResource;
      this.selected = false;
      // although we do the judgement here, the task should be validated before executing because
      // the status of file may be changed after the task is submitted to queue
      this.isValidCandidate = tsFileResource.getStatus() == TsFileResourceStatus.NORMAL;
    }

    /**
     * The TsFile is unsealed means there may be more data which will be inserted into this file.
     *
     * @return Whether the TsFile is unsealed.
     */
    protected boolean unsealed() {
      return resource.getStatus() == TsFileResourceStatus.UNCLOSED;
    }

    private void prepareDeviceInfos() throws IOException {
      if (deviceInfoMap != null) {
        return;
      }
      deviceInfoMap = new LinkedHashMap<>();
      if (resource.getTimeIndexType() == ITimeIndex.FILE_TIME_INDEX_TYPE) {
        // deserialize resource file
        resource.readLock();
        try {
          if (!resource.resourceFileExists()) {
            hasDetailedDeviceInfo = false;
            return;
          }
          DeviceTimeIndex timeIndex = resource.buildDeviceTimeIndex();
          for (String deviceId : timeIndex.getDevices()) {
            deviceInfoMap.put(
                deviceId,
                new DeviceInfo(
                    deviceId, timeIndex.getStartTime(deviceId), timeIndex.getEndTime(deviceId)));
          }
        } finally {
          resource.readUnlock();
        }
      } else {
        for (String deviceId : resource.getDevices()) {
          deviceInfoMap.put(
              deviceId,
              new DeviceInfo(
                  deviceId, resource.getStartTime(deviceId), resource.getEndTime(deviceId)));
        }
      }
      hasDetailedDeviceInfo = true;
    }

    public void markAsSelected() {
      this.selected = true;
    }

    protected List<DeviceInfo> getDevices() throws IOException {
      prepareDeviceInfos();
      return new ArrayList<>(deviceInfoMap.values());
    }

    protected DeviceInfo getDeviceInfoById(String deviceId) throws IOException {
      prepareDeviceInfos();
      return deviceInfoMap.get(deviceId);
    }

    protected boolean containsDevice(String deviceId) throws IOException {
      prepareDeviceInfos();
      return deviceInfoMap.containsKey(deviceId);
    }

    protected boolean hasDetailedDeviceInfo() throws IOException {
      prepareDeviceInfos();
      return hasDetailedDeviceInfo;
    }

    protected boolean mayHasOverlapWithUnseqFile(DeviceInfo unseqFileDeviceInfo)
        throws IOException {
      prepareDeviceInfos();
      long endTime =
          containsDevice(unseqFileDeviceInfo.deviceId)
              ? getDeviceInfoById(unseqFileDeviceInfo.deviceId).endTime
              : resource.getFileEndTime();
      return unseqFileDeviceInfo.startTime <= endTime;
    }
  }

  protected static class DeviceInfo {
    protected String deviceId;
    protected long startTime;
    protected long endTime;

    public DeviceInfo(String deviceId, long startTime, long endTime) {
      this.deviceId = deviceId;
      this.startTime = startTime;
      this.endTime = endTime;
    }
  }
}
