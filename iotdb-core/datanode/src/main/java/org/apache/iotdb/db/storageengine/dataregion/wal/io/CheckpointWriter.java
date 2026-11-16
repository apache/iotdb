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

package org.apache.iotdb.db.storageengine.dataregion.wal.io;

import org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint.Checkpoint;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/** CheckpointWriter writes the binary {@link Checkpoint} into .checkpoint file. */
public class CheckpointWriter extends LogWriter {
  private File currentFile;

  public CheckpointWriter(File logFile) throws IOException {
    super(logFile, WALFormats.writeVersion(WALFileVersion.V2));
    currentFile = logFile;
  }

  /** Publishes a forced checkpoint in the same directory, keeping its writer open for appends. */
  public void publish(File targetFile) throws IOException {
    Path target = targetFile.toPath();
    // The manager owns the directory. Check for a stale target since ATOMIC_MOVE may replace it.
    if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
      throw new FileAlreadyExistsException(target.toString());
    }
    Files.move(currentFile.toPath(), target, StandardCopyOption.ATOMIC_MOVE);
    currentFile = targetFile;
  }

  @Override
  public File getLogFile() {
    return currentFile;
  }
}
