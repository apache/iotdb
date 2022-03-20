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
package org.apache.iotdb.db.wal.io;

import org.apache.iotdb.db.wal.checkpoint.Checkpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/** CheckpointReader is used to read all checkpoints from .checkpoint file. */
public class CheckpointReader {
  private static final Logger logger = LoggerFactory.getLogger(CheckpointReader.class);

  private final File logFile;

  public CheckpointReader(File logFile) {
    this.logFile = logFile;
  }

  /**
   * Read all checkpoints from .checkpoint file.
   *
   * @return checkpoints
   */
  public List<Checkpoint> readAll() {
    List<Checkpoint> checkpoints = new LinkedList<>();
    try (DataInputStream logStream =
        new DataInputStream(new BufferedInputStream(new FileInputStream(logFile)))) {
      while (logStream.available() > 0) {
        Checkpoint checkpoint = Checkpoint.deserialize(logStream);
        checkpoints.add(checkpoint);
      }
    } catch (FileNotFoundException e) {
      logger.warn("Checkpoint file {} doesn't exist.", logFile, e);
    } catch (IOException e) {
      logger.warn("Meet error when deserializing checkpoint file {}", logFile, e);
    }
    return checkpoints;
  }
}
