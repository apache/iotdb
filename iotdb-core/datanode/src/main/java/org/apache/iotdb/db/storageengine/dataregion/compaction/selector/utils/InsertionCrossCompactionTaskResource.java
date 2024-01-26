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

import java.util.ArrayList;
import java.util.List;

public class InsertionCrossCompactionTaskResource extends CrossCompactionTaskResource {
  public TsFileResource prevSeqFile = null;
  public TsFileResource nextSeqFile = null;
  public TsFileResource toInsertUnSeqFile = null;
  public TsFileResource firstUnSeqFileInParitition = null;
  public long targetFileTimestamp;

  public void setToInsertUnSeqFile(TsFileResource toInsertUnSeqFile) {
    this.toInsertUnSeqFile = toInsertUnSeqFile;
  }

  @Override
  public float getTotalSeqFileSize() {
    return super.getTotalSeqFileSize();
  }

  @Override
  public List<TsFileResource> getSeqFiles() {
    List<TsFileResource> seqFiles = new ArrayList<>(2);
    if (prevSeqFile != null) {
      seqFiles.add(prevSeqFile);
    }
    if (nextSeqFile != null) {
      seqFiles.add(nextSeqFile);
    }
    return seqFiles;
  }

  @Override
  public List<TsFileResource> getUnseqFiles() {
    List<TsFileResource> unseqFiles = new ArrayList<>(2);
    unseqFiles.add(firstUnSeqFileInParitition);
    if (toInsertUnSeqFile != firstUnSeqFileInParitition) {
      unseqFiles.add(toInsertUnSeqFile);
    }
    return unseqFiles;
  }

  @Override
  public boolean isValid() {
    return toInsertUnSeqFile != null;
  }
}
