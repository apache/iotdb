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
package org.apache.iotdb.tsfile.fileSystem.fileOutputFactory;

import org.apache.iotdb.tsfile.fileSystem.FSPath;
import org.apache.iotdb.tsfile.fileSystem.FSType;
import org.apache.iotdb.tsfile.utils.FSUtils;
import org.apache.iotdb.tsfile.write.writer.TsFileOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HybridFileOutputFactory implements FileOutputFactory {
  private static final Logger logger = LoggerFactory.getLogger(HybridFileOutputFactory.class);
  private static final Map<FSType, FileOutputFactory> outputFactories = new ConcurrentHashMap<>();

  static {
    outputFactories.put(FSType.LOCAL, new LocalFSOutputFactory());
    outputFactories.put(FSType.HDFS, new HDFSOutputFactory());
    outputFactories.put(FSType.OBJECT_STORAGE, new OSFileOutputFactory());
  }

  @Override
  public TsFileOutput getTsFileOutput(String filePath, boolean append) {
    FSPath path = FSUtils.parse(filePath);
    return outputFactories.get(path.getFsType()).getTsFileOutput(path.getPath(), append);
  }
}
