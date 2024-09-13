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

package org.apache.iotdb.db.storageengine.dataregion.wal.recover.file;

import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.exception.NotCompatibleTsFileException;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/** This class is used to help recover TsFile. */
public abstract class AbstractTsFileRecoverPerformer implements Closeable {
  private static final Logger logger =
      LoggerFactory.getLogger(AbstractTsFileRecoverPerformer.class);

  // TsFile which needs recovery
  protected final TsFileResource tsFileResource;
  // this writer will be open when .resource file doesn't exist
  protected RestorableTsFileIOWriter writer;

  protected AbstractTsFileRecoverPerformer(TsFileResource tsFileResource) {
    this.tsFileResource = tsFileResource;
  }

  /**
   * Recover TsFile with RestorableTsFileIOWriter, including load .resource file (reconstruct when
   * necessary) and truncate the file to remaining corrected data. <br>
   * Notice: this method may open a {@link RestorableTsFileIOWriter}, remember to close it.
   *
   * @throws DataRegionException when failing to new RestorableTsFileIOWriter.
   * @throws IOException when failing to read .resource file.
   */
  protected void recoverWithWriter() throws DataRegionException, IOException {
    File tsFile = tsFileResource.getTsFile();
    File chunkMetadataTempFile =
        new File(tsFile.getAbsolutePath() + TsFileIOWriter.CHUNK_METADATA_TEMP_FILE_SUFFIX);
    if (chunkMetadataTempFile.exists()) {
      // delete chunk metadata temp file
      FileUtils.delete(chunkMetadataTempFile);
    }

    if (tsFileResource.resourceFileExists()) {
      // .resource file exists, just deserialize it into memory
      loadResourceFile();
      return;
    }

    if (!tsFile.exists()) {
      logger.error("TsFile {} is missing, will skip its recovery.", tsFile);
      return;
    }

    // try to remove corrupted part of the TsFile
    try {
      writer = new RestorableTsFileIOWriter(tsFile);
    } catch (NotCompatibleTsFileException e) {
      boolean result = tsFile.delete();
      logger.warn(
          "TsFile {} is incompatible. Try to delete it and delete result is {}", tsFile, result);
      // if the broken TsFile is v3, we can recover the all data from wal
      // to support it, we can regenerate an empty file here
      Files.createFile(tsFile.toPath());
      writer = new RestorableTsFileIOWriter(tsFile);
      throw new DataRegionException(e);
    } catch (IOException e) {
      throw new DataRegionException(e);
    }

    // reconstruct .resource file when TsFile is complete
    if (!writer.hasCrashed()) {
      try {
        reconstructResourceFile();
      } catch (IOException e) {
        throw new DataRegionException(
            "Failed recover the resource file: " + tsFile + TsFileResource.RESOURCE_SUFFIX + e);
      }
    }
  }

  private void loadResourceFile() throws IOException {
    try {
      tsFileResource.deserialize();
    } catch (Throwable e) {
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
      TsFileResourceUtils.updateTsFileResource(reader, tsFileResource);
    }

    // set progress index for pipe to avoid data loss
    PipeDataNodeAgent.runtime().assignProgressIndexForTsFileRecovery(tsFileResource);

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
