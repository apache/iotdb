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
package org.apache.tsfile.fileSystem.fileInputFactory;

import org.apache.tsfile.fileSystem.FSPath;
import org.apache.tsfile.fileSystem.FSType;
import org.apache.tsfile.read.reader.TsFileInput;
import org.apache.tsfile.utils.FSUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HybridFileInputFactory implements FileInputFactory {
  private static final Logger logger = LoggerFactory.getLogger(HybridFileInputFactory.class);
  private static final Map<FSType, FileInputFactory> inputFactories = new ConcurrentHashMap<>();

  private FileInputFactory getFileInputFactory(FSType fsType) {
    return inputFactories.compute(
        fsType,
        (k, v) -> {
          if (v != null) {
            return v;
          }
          switch (fsType) {
            case LOCAL:
              return new LocalFSInputFactory();
            case OBJECT_STORAGE:
              return new OSFileInputFactory();
            case HDFS:
              return new HDFSInputFactory();
            default:
              return null;
          }
        });
  }

  @Override
  public TsFileInput getTsFileInput(String filePath) throws IOException {
    FSPath path = FSUtils.parse(filePath);
    return getFileInputFactory(path.getFsType()).getTsFileInput(path.getPath());
  }
}
