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

import org.apache.iotdb.tsfile.write.writer.LocalTsFileOutput;
import org.apache.iotdb.tsfile.write.writer.TsFileOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class LocalFSOutputFactory implements FileOutputFactory {

  private static final Logger logger = LoggerFactory.getLogger(LocalFSOutputFactory.class);

  @Override
  public TsFileOutput getTsFileOutput(String filePath, boolean append) {
    try {
      return new LocalTsFileOutput(new FileOutputStream(new File(filePath), append));
    } catch (IOException e) {
      logger.error("Failed to get TsFile output of file: {}, ", filePath, e);
      return null;
    }
  }
}
