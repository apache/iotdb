/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.storagegroup;

import org.apache.iotdb.db.engine.storagegroup.timeindex.FileTimeIndex;
import org.apache.iotdb.db.engine.storagegroup.timeindex.ITimeIndex;

import java.io.File;

public class FakedTsFileResource extends TsFileResource {
  /** time index */
  protected ITimeIndex timeIndex;

  private long tsFileSize;
  private String fakeTsfileName;

  public FakedTsFileResource(long tsFileSize, String name) {
    this.tsFileSize = tsFileSize;
    super.closed = true;
    super.isMerging = false;
    fakeTsfileName = name;
  }

  public FakedTsFileResource(long tsFileSize, boolean isClosed, boolean isMerging, String name) {
    this.tsFileSize = tsFileSize;
    super.closed = isClosed;
    super.isMerging = isMerging;
    fakeTsfileName = name;
  }

  public FakedTsFileResource(long tsFileSize, long startTime, long endTime) {
    this.timeIndex = new FileTimeIndex();
    timeIndex.putStartTime("", startTime);
    timeIndex.putEndTime("", endTime);
    this.tsFileSize = tsFileSize;
  }

  public void setTsFileSize(long tsFileSize) {
    this.tsFileSize = tsFileSize;
  }

  @Override
  public long getTsFileSize() {
    return tsFileSize;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(tsFileSize).append(",");
    builder.append(closed).append(",");
    builder.append(isMerging);
    return builder.toString();
  }

  @Override
  public File getTsFile() {
    return new File(fakeTsfileName);
  }
}
