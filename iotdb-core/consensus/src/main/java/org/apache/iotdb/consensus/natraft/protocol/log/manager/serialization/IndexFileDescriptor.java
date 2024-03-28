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
package org.apache.iotdb.consensus.natraft.protocol.log.manager.serialization;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.apache.iotdb.commons.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.iotdb.consensus.natraft.protocol.log.manager.serialization.SyncLogDequeSerializer.FILE_NAME_PART_LENGTH;

public class IndexFileDescriptor {

  private static final Logger logger = LoggerFactory.getLogger(IndexFileDescriptor.class);
  File file;
  long startIndex;
  long endIndex;

  public IndexFileDescriptor(File file) {
    this.file = file;
    String[] splits = file.getName().split(FILE_NAME_SEPARATOR);
    if (splits.length != FILE_NAME_PART_LENGTH) {
      logger.error(
          "file={} name should be in the following format: startLogIndex-endLogIndex-version-idx",
          file.getAbsoluteFile());
    }
    this.startIndex = Long.parseLong(splits[0]);
    this.endIndex = Long.parseLong(splits[1]);
  }

  public IndexFileDescriptor(File file, long startIndex, long endIndex) {
    this.file = file;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }
}
