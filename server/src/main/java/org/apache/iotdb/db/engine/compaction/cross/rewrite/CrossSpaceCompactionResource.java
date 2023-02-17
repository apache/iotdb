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

/**
 * CrossSpaceCompactionResource manages files and caches of readers to avoid unnecessary object
 * creations and file openings.
 */
public class CrossSpaceCompactionResource {
  private List<TsFileResource> seqFiles;
  private List<TsFileResource> unseqFiles = new ArrayList<>();

  private long ttlLowerBound = Long.MIN_VALUE;

  public CrossSpaceCompactionResource(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    this.seqFiles = seqFiles;
    filterUnseqResource(unseqFiles);
  }

  /**
   * Filter the unseq files into the compaction. Unseq files should be not deleted or over ttl. To
   * ensure that the compaction is correct, return as soon as it encounters the file being compacted
   * or compaction candidate. Therefore, a cross space compaction can only be performed serially
   * under a time partition in a VSG.
   */
  private void filterUnseqResource(List<TsFileResource> unseqResources) {
    for (TsFileResource resource : unseqResources) {
      if (resource.getStatus() != TsFileResourceStatus.CLOSED) {
        return;
      } else if (!resource.isDeleted() && resource.stillLives(ttlLowerBound)) {
        this.unseqFiles.add(resource);
      }
    }
  }

  public CrossSpaceCompactionResource(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles, long ttlLowerBound) {
    this.ttlLowerBound = ttlLowerBound;
    this.seqFiles = seqFiles;
    filterUnseqResource(unseqFiles);
  }

  public List<TsFileResource> getSeqFiles() {
    return seqFiles;
  }

  public List<TsFileResource> getUnseqFiles() {
    return unseqFiles;
  }
}
