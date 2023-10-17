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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** MergeLogger records the progress of a merge in file "merge.log" as text lines. */
public class InnerCompactionLogger extends CompactionLogger {

  private List<TsFileResource> sourceFiles;
  private TsFileResource targetFile;
  private TsFileResource emptyTargetFile;

  public InnerCompactionLogger(File logFile) throws IOException {
    super(logFile);
    this.sourceFiles = new ArrayList<>();
  }

  public List<TsFileResource> getSourceFiles() {
    return sourceFiles;
  }

  public TsFileResource getTargetFile() {
    return targetFile;
  }

  public TsFileResource getEmptyTargetFile() {
    return emptyTargetFile;
  }

  public void logSourceFiles(List<TsFileResource> sourceFiles) throws IOException {
    this.sourceFiles.addAll(sourceFiles);
    logFiles(sourceFiles, STR_SOURCE_FILES);
  }

  public void logTargetFile(TsFileResource targetFile) throws IOException {
    this.targetFile = targetFile;
    logFile(targetFile, STR_TARGET_FILES);
  }

  public void logEmptyTargetFile(TsFileResource emptyTargetFile) throws IOException {
    this.emptyTargetFile = emptyTargetFile;
    logFile(emptyTargetFile, STR_DELETED_TARGET_FILES);
  }
}
