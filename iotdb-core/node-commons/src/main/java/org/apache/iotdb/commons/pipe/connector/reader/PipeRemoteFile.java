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

package org.apache.iotdb.commons.pipe.connector.reader;

import java.io.File;

/**
 * This class represents a remote object stored TsFile extracted by HistoricalExtractor. It extends
 * File to provide compatibility with Pipe framework.
 */
public class PipeRemoteFile extends File {
  private final File tsFile;
  // hardLinkOrCopiedFile is a virtual placeholder file used to provide consistent naming
  // behavior for pipe framework compatibility. It does not exist on disk.
  private final File hardLinkOrCopiedFile;

  public PipeRemoteFile(File tsfile, File hardLinkOrCopiedFile) {
    super(tsfile.getAbsolutePath());
    this.tsFile = tsfile;
    this.hardLinkOrCopiedFile = hardLinkOrCopiedFile;
  }

  @Override
  public String getName() {
    return hardLinkOrCopiedFile.getName();
  }

  @Override
  public String getPath() {
    return tsFile.getPath();
  }

  public String getHardLinkOrCopiedFilePath() {
    return hardLinkOrCopiedFile.getPath();
  }

  @Override
  public String toString() {
    return "PipeRemoteTsFile{"
        + "tsFile="
        + tsFile
        + ", hardLinkOrCopiedFile="
        + hardLinkOrCopiedFile
        + '}';
  }
}
