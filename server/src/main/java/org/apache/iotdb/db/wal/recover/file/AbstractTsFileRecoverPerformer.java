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
package org.apache.iotdb.db.wal.recover.file;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.exception.NotCompatibleTsFileException;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import static org.apache.iotdb.db.engine.storagegroup.TsFileResource.RESOURCE_SUFFIX;

/** This class is used to help recover TsFile */
public abstract class AbstractTsFileRecoverPerformer implements Closeable {
  private static final Logger logger =
      LoggerFactory.getLogger(AbstractTsFileRecoverPerformer.class);

  /** TsFile which needs recovery */
  protected final TsFileResource tsFileResource;
  /** this writer will be open when .resource file doesn't exist */
  protected RestorableTsFileIOWriter writer;

  public AbstractTsFileRecoverPerformer(TsFileResource tsFileResource) {
    this.tsFileResource = tsFileResource;
  }

  /**
   * Recover TsFile with RestorableTsFileIOWriter, including load .resource file (reconstruct when
   * necessary) and truncate the file to remaining corrected data. <br>
   * Notice: this method may open a {@link RestorableTsFileIOWriter}, remember to close it.
   */
  protected void recoverWithWriter() throws StorageGroupProcessorException, IOException {
    File tsFile = tsFileResource.getTsFile();
    if (!tsFile.exists()) {
      logger.error("TsFile {} is missing, will skip its recovery.", tsFile);
      return;
    }

    if (tsFileResource.resourceFileExists()) {
      // .resource file exists, just deserialize it into memory
      loadResourceFile();
      return;
    }

    // try to remove corrupted part of the TsFile
    try {
      writer = new RestorableTsFileIOWriter(tsFile);
    } catch (NotCompatibleTsFileException e) {
      boolean result = tsFile.delete();
      logger.warn(
          "TsFile {} is incompatible. Try to delete it and delete result is {}", tsFile, result);
      throw new StorageGroupProcessorException(e);
    } catch (IOException e) {
      throw new StorageGroupProcessorException(e);
    }

    // reconstruct .resource file when TsFile is complete
    if (!writer.hasCrashed()) {
      try {
        reconstructResourceFile();
      } catch (IOException e) {
        throw new StorageGroupProcessorException(
            "Failed recover the resource file: " + tsFile + RESOURCE_SUFFIX + e);
      }
    }
  }

  private void loadResourceFile() throws IOException {
    try {
      tsFileResource.deserialize();
    } catch (IOException e) {
      logger.warn(
          "Cannot deserialize .resource file of {}, try to reconstruct it.",
          tsFileResource.getTsFile(),
          e);
      reconstructResourceFile();
    }
  }

  protected void reconstructResourceFile() throws IOException {
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(tsFileResource.getTsFile().getAbsolutePath())) {
      FileLoaderUtils.updateTsFileResource(reader, tsFileResource);
    }
    tsFileResource.serialize();
  }

  public boolean hasCrashed() {
    return writer != null && writer.hasCrashed();
  }

  public boolean canWrite() {
    return writer != null && writer.canWrite();
  }

  @Override
  public void close() throws IOException {
    if (writer != null) {
      writer.close();
    }
  }
}
