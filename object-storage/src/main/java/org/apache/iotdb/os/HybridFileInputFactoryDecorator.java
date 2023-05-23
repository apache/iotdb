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
package org.apache.iotdb.os;

import org.apache.iotdb.os.conf.ObjectStorageConfig;
import org.apache.iotdb.os.conf.ObjectStorageDescriptor;
import org.apache.iotdb.os.io.aws.AWSS3Config;
import org.apache.iotdb.tsfile.fileSystem.fileInputFactory.FileInputFactory;
import org.apache.iotdb.tsfile.fileSystem.fileInputFactory.HybridFileInputFactory;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.utils.FSUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class HybridFileInputFactoryDecorator implements FileInputFactory {
  private static final Logger logger =
      LoggerFactory.getLogger(HybridFileInputFactoryDecorator.class);
  private static final ObjectStorageConfig config =
      ObjectStorageDescriptor.getInstance().getConfig();
  private static final FileInputFactory fileInputFactory = new HybridFileInputFactory();

  private int dataNodeId;

  public HybridFileInputFactoryDecorator(int dataNodeId) {
    this.dataNodeId = dataNodeId;
  }

  @Override
  public TsFileInput getTsFileInput(String filePath) throws IOException {
    File file = new File(filePath);
    if (!file.exists()) {
      return fileInputFactory.getTsFileInput(
          FSUtils.parseLocalTsFile2OSFile(file, AWSS3Config.getBucketName(), dataNodeId).getPath());
    }
    return fileInputFactory.getTsFileInput(filePath);
  }
}
