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

package org.apache.iotdb.db.pipe.resource.tsfile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;

public class PipeTsFileResource implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsFileResource.class);

  private final File hardlinkOrCopiedFile;

  private volatile long fileSize = -1L;

  private final AtomicInteger referenceCount;

  public PipeTsFileResource(final File hardlinkOrCopiedFile) {
    this.hardlinkOrCopiedFile = hardlinkOrCopiedFile;

    referenceCount = new AtomicInteger(1);
  }

  public File getFile() {
    return hardlinkOrCopiedFile;
  }

  public long getFileSize() {
    if (fileSize == -1L) {
      synchronized (this) {
        if (fileSize == -1L) {
          fileSize = hardlinkOrCopiedFile.length();
        }
      }
    }
    return fileSize;
  }

  ///////////////////// Reference Count /////////////////////

  public int getReferenceCount() {
    return referenceCount.get();
  }

  public void increaseReferenceCount() {
    referenceCount.addAndGet(1);
  }

  public boolean decreaseReferenceCount() {
    final int finalReferenceCount = referenceCount.addAndGet(-1);
    if (finalReferenceCount == 0) {
      close();
      return true;
    }
    if (finalReferenceCount < 0) {
      LOGGER.warn("PipeTsFileResource's reference count is decreased to below 0.");
    }
    return false;
  }

  @Override
  public synchronized void close() {
    boolean successful = false;
    try {
      successful = Files.deleteIfExists(hardlinkOrCopiedFile.toPath());
    } catch (final Exception e) {
      LOGGER.error(
          "PipeTsFileResource: Failed to delete tsfile {} when closing, because {}. Please MANUALLY delete it.",
          hardlinkOrCopiedFile,
          e.getMessage(),
          e);
    }

    if (successful) {
      LOGGER.info("PipeTsFileResource: Closed tsfile {} and cleaned up.", hardlinkOrCopiedFile);
    }
  }
}
