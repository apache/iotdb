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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.RepairUnsortedFileCompactionWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

/** Used for fixing files which contains internal unsorted data */
public class RepairUnsortedFileCompactionPerformer extends ReadPointCompactionPerformer {

  private final boolean rewriteFile;

  public RepairUnsortedFileCompactionPerformer(boolean rewriteFile) {
    super();
    this.rewriteFile = rewriteFile;
  }

  @Override
  protected AbstractCompactionWriter getCompactionWriter(
      List<TsFileResource> seqFileResources,
      List<TsFileResource> unseqFileResources,
      List<TsFileResource> targetFileResources)
      throws IOException {
    return new RepairUnsortedFileCompactionWriter(targetFileResources.get(0));
  }

  @Override
  public void perform() throws Exception {
    if (rewriteFile) {
      super.perform();
    } else {
      prepareTargetFile();
    }
  }

  private void prepareTargetFile() throws IOException {
    TsFileResource seqSourceFile = seqFiles.get(0);
    TsFileResource targetFile = targetFiles.get(0);
    Files.createLink(targetFile.getTsFile().toPath(), seqSourceFile.getTsFile().toPath());
    ITimeIndex timeIndex = seqSourceFile.getTimeIndex();
    if (timeIndex instanceof DeviceTimeIndex) {
      targetFile.setTimeIndex(timeIndex);
    } else {
      targetFile.setTimeIndex(seqSourceFile.buildDeviceTimeIndex());
    }
    if (seqSourceFile.modFileExists()) {
      Files.createLink(
          new File(seqSourceFile.getCompactionModFile().getFilePath()).toPath(),
          new File(seqSourceFile.getModFile().getFilePath()).toPath());
    }
  }

  @Override
  public void setSourceFiles(List<TsFileResource> sourceFiles) {
    if (sourceFiles.get(0).isSeq()) {
      seqFiles = sourceFiles;
    } else {
      unseqFiles = sourceFiles;
    }
  }
}
