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
package org.apache.iotdb.db.newsync.pipedata;

import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.newsync.receiver.load.ILoader;
import org.apache.iotdb.db.newsync.receiver.load.TsFileLoader;
import org.apache.iotdb.db.newsync.sender.conf.SenderConf;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TsFilePipeData extends PipeData {
  private static final Logger logger = LoggerFactory.getLogger(TsFilePipeData.class);

  private String tsFilePath;

  public TsFilePipeData(String tsFilePath, long serialNumber) {
    super(serialNumber);
    this.tsFilePath = tsFilePath;
  }

  @Override
  public Type getType() {
    return Type.TSFILE;
  }

  @Override
  public long serialize(DataOutputStream stream) throws IOException {
    return super.serialize(stream) + ReadWriteIOUtils.write(tsFilePath, stream);
  }

  public static TsFilePipeData deserialize(DataInputStream stream) throws IOException {
    long serialNumber = stream.readLong();
    String tsFilePath = ReadWriteIOUtils.readString(stream);
    return new TsFilePipeData(tsFilePath, serialNumber);
  }

  @Override
  public ILoader createLoader() {
    return new TsFileLoader(new File(tsFilePath));
  }

  @Override
  public void sendToTransport() {
    if (waitForTsFileClose()) {
      // senderTransprot(getFiles(), this);
      System.out.println(this);
    }
  }

  private boolean waitForTsFileClose() {
    for (int i = 0; i < SenderConf.defaultWaitingForTsFileRetryNumber; i++) {
      if (isTsFileClosed()) {
        return true;
      }
      try {
        Thread.sleep(SenderConf.defaultWaitingForTsFileCloseMilliseconds);
      } catch (InterruptedException e) {
        logger.warn(String.format("Be Interrupted when waiting for tsfile %s closed", tsFilePath));
      }
      logger.info(
          String.format(
              "Waiting for tsfile %s close, retry %d / %d.",
              tsFilePath, (i + 1), SenderConf.defaultWaitingForTsFileRetryNumber));
    }
    return false;
  }

  private boolean isTsFileClosed() {
    File tsFile = new File(tsFilePath).getAbsoluteFile();
    File resource = new File(tsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX);
    return resource.exists();
  }

  public List<File> getTsFiles() throws FileNotFoundException {
    File tsFile = new File(tsFilePath).getAbsoluteFile();
    File resource = new File(tsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX);
    File mods = new File(tsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX);

    List<File> files = new ArrayList<>();
    if (!tsFile.exists()) {
      throw new FileNotFoundException(String.format("Can not find %s.", tsFile.getAbsolutePath()));
    }
    files.add(tsFile);
    if (resource.exists()) {
      files.add(resource);
    }
    if (mods.exists()) {
      files.add(mods);
    }
    return files;
  }

  @Override
  public String toString() {
    return "TsFilePipeData{"
        + "serialNumber="
        + serialNumber
        + ", tsFilePath='"
        + tsFilePath
        + '\''
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TsFilePipeData pipeData = (TsFilePipeData) o;
    return Objects.equals(tsFilePath, pipeData.tsFilePath)
        && Objects.equals(serialNumber, pipeData.serialNumber);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tsFilePath, serialNumber);
  }
}
