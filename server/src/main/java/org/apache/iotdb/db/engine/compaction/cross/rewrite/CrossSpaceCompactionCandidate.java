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

package org.apache.iotdb.db.engine.compaction.cross.rewrite;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;

import java.util.ArrayList;
import java.util.List;
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

  public boolean hasNextSplit() {
    if (nextUnseqFileIndex >= unseqFiles.size()) {
      return false;
    }
    return prepareNextSplit();
  }

  public CrossCompactionTaskResourceSplit nextSplit() {
    return nextSplit;
  }

  private boolean prepareNextSplit() {
    TsFileResourceCandidate unseqFile = unseqFiles.get(nextUnseqFileIndex);
    List<TsFileResourceCandidate> ret = new ArrayList<>();

    for (DeviceInfo unseqDeviceInfo : unseqFile.getDevices()) {
      for (TsFileResourceCandidate seqFile : seqFiles) {
        if (!seqFile.containsDevice(unseqDeviceInfo.deviceId)) {
          continue;
        }
        DeviceInfo seqDeviceInfo = seqFile.getDeviceInfoById(unseqDeviceInfo.deviceId);
        if (!seqFile.isValidCandidate) {
          // If the unclosed seqFile should be selected, the whole selection should be terminated
          if (unseqDeviceInfo.endTime >= seqDeviceInfo.startTime) {
            return false;
          }
        }
        if (unseqDeviceInfo.endTime <= seqDeviceInfo.endTime) {
          // When scanning the target seqFiles for unseqFile, we traverse them one by one no matter
          // whether it is selected or not. But we only add the unselected seqFiles to next split to
          // avoid duplication selection
          if (!seqFile.selected) {
            ret.add(seqFile);
            seqFile.markAsSelected();
          }
          // if this condition is satisfied, all subsequent seq files is unnecessary to check
          break;
        } else if (unseqDeviceInfo.startTime <= seqDeviceInfo.endTime) {
          if (!seqFile.selected) {
            ret.add(seqFile);
            seqFile.markAsSelected();
          }
        }
      }
    }
    // mark candidates in next split as selected even though it may not be added to the final
    // TaskResource
    unseqFile.markAsSelected();
    nextSplit = new CrossCompactionTaskResourceSplit(unseqFile, ret);
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
      if (resource.getStatus() != TsFileResourceStatus.CLOSED
          || !resource.getTsFile().exists()
          || resource.isDeleted()) {
        break;
      } else if (!resource.isDeleted() && resource.stillLives(ttlLowerBound)) {
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

  public List<TsFileResourceCandidate> getUnseqFileCandidates() {
    return unseqFiles;
  }

  public List<TsFileResource> getUnseqFiles() {
    return unseqFiles.stream()
        .map(tsFileResourceCandidate -> tsFileResourceCandidate.resource)
        .collect(Collectors.toList());
  }

  protected static class CrossCompactionTaskResourceSplit {
    protected TsFileResourceCandidate unseqFile;
    protected List<TsFileResourceCandidate> seqFiles;

    public CrossCompactionTaskResourceSplit(
        TsFileResourceCandidate unseqFile, List<TsFileResourceCandidate> seqFiles) {
      this.unseqFile = unseqFile;
      this.seqFiles = seqFiles;
    }
  }

  protected static class TsFileResourceCandidate {
    protected TsFileResource resource;
    protected boolean selected;
    protected boolean isValidCandidate;

    protected TsFileResourceCandidate(TsFileResource tsFileResource) {
      this.resource = tsFileResource;
      this.selected = false;
      // although we do the judgement here, the task should be validated before executing because
      // the status of file may be changed after the task is submitted to queue
      this.isValidCandidate = tsFileResource.isClosed() && tsFileResource.getTsFile().exists();
    }

    protected void markAsSelected() {
      this.selected = true;
    }

    protected List<DeviceInfo> getDevices() {
      return null;
    }

    protected DeviceInfo getDeviceInfoById(String deviceId) {
      return null;
    }

    protected boolean containsDevice(String deviceId) {
      return false;
    }
  }

  protected static class DeviceInfo {
    protected String deviceId;
    protected long startTime;
    protected long endTime;
  }
}
