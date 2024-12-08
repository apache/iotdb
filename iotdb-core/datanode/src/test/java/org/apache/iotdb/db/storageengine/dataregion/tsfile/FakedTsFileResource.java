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

package org.apache.iotdb.db.storageengine.dataregion.tsfile;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.FileTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;

import java.io.File;

public class FakedTsFileResource extends TsFileResource {
  /** time index */
  public ITimeIndex timeIndex;

  public long timePartition;

  private long tsFileSize;
  private String fakeTsfileName;

  public FakedTsFileResource(long tsFileSize, String name) {
    super(new File(name));
    this.timeIndex = new FileTimeIndex();
    this.tsFileSize = tsFileSize;
    fakeTsfileName = name;
    setStatusForTest(TsFileResourceStatus.NORMAL);
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
    builder.append(getStatus());
    return builder.toString();
  }

  @Override
  public File getTsFile() {
    return new File(fakeTsfileName);
  }

  @Override
  public boolean equals(Object otherObject) {
    if (otherObject instanceof FakedTsFileResource) {
      FakedTsFileResource otherResource = (FakedTsFileResource) otherObject;
      return this.fakeTsfileName.equals(otherResource.fakeTsfileName)
          && this.tsFileSize == otherResource.tsFileSize;
    }

    return false;
  }

  @Override
  public long getTimePartition() {
    return this.timePartition;
  }
}
