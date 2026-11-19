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

package org.apache.iotdb.db.storageengine.dataregion.snapshot;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

final class SnapshotFileUtils {
  private SnapshotFileUtils() {}

  static void createLinkOrCopy(Path target, Path source) throws IOException {
    try {
      Files.createLink(target, source);
    } catch (IOException | UnsupportedOperationException linkFailure) {
      // Snapshot sources are immutable or protected by the caller's file locks.
      // Do not replace an existing destination: it may belong to another capture.
      try {
        Files.copy(source, target);
        // Persist the new inode before the snapshot log can mark this file as captured.
        try (FileChannel output = FileChannel.open(target, StandardOpenOption.WRITE)) {
          output.force(true);
        }
      } catch (IOException | UnsupportedOperationException copyFailure) {
        copyFailure.addSuppressed(linkFailure);
        throw copyFailure;
      }
    }
  }
}
