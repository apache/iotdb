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
 *
 */

package org.apache.iotdb.db.pipe.sink.payload.legacy;

import org.apache.iotdb.db.pipe.receiver.protocol.legacy.loader.ILoader;
import org.apache.iotdb.db.pipe.receiver.protocol.legacy.loader.TsFileLoader;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Objects;

public class TsFilePipeData extends PipeData {

  private String parentDirPath;
  private String tsFileName;
  private String database;

  public TsFilePipeData() {
    super();
  }

  public TsFilePipeData(String parentDirPath, String tsFileName, long serialNumber) {
    super(serialNumber);
    this.parentDirPath = parentDirPath;
    this.tsFileName = tsFileName;

    initDatabaseName();
  }

  // == get Database Info from tsFileName
  private void initDatabaseName() {
    if (tsFileName == null) {
      database = null;
      return;
    }

    String[] parts = tsFileName.trim().split("-");
    if (parts.length < 8) {
      database = null;
      return;
    }
    StringBuilder stringBuilder = new StringBuilder(parts[1]);
    for (int i = 2; i < (parts.length - 6); i++) {
      stringBuilder.append("-").append(parts[i]);
    }
    database = stringBuilder.toString();
  }

  public void setParentDirPath(String parentDirPath) {
    this.parentDirPath = parentDirPath;
  }

  public String getTsFileName() {
    return tsFileName;
  }

  public String getTsFilePath() {
    return parentDirPath + File.separator + tsFileName;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getDatabase() {
    return database;
  }

  @Override
  public PipeDataType getPipeDataType() {
    return PipeDataType.TSFILE;
  }

  @Override
  public long serialize(DataOutputStream stream) throws IOException {
    return super.serialize(stream)
        + ReadWriteIOUtils.write(parentDirPath, stream)
        + ReadWriteIOUtils.write(tsFileName, stream);
  }

  @Override
  public void deserialize(DataInputStream stream) throws IOException {
    super.deserialize(stream);
    parentDirPath = ReadWriteIOUtils.readString(stream);
    if (parentDirPath == null) {
      parentDirPath = "";
    }
    tsFileName = ReadWriteIOUtils.readString(stream);
    initDatabaseName();
  }

  @Override
  public ILoader createLoader() {
    return new TsFileLoader(new File(getTsFilePath()), database);
  }

  @Override
  public String toString() {
    return "TsFilePipeData{"
        + "serialNumber="
        + serialNumber
        + ", tsFilePath='"
        + getTsFilePath()
        + '\''
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TsFilePipeData pipeData = (TsFilePipeData) o;
    return Objects.equals(parentDirPath, pipeData.parentDirPath)
        && Objects.equals(tsFileName, pipeData.tsFileName)
        && Objects.equals(serialNumber, pipeData.serialNumber);
  }

  @Override
  public int hashCode() {
    return Objects.hash(parentDirPath, tsFileName, serialNumber);
  }
}
