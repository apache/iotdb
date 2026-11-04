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

import org.apache.iotdb.commons.utils.IOUtils;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;
import org.apache.iotdb.db.utils.EncryptDBUtils;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.exception.NotCompatibleTsFileException;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

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
    boolean reinitialize =
        tsFile.exists() && !tsFileResource.resourceFileExists() && hasNoPersistedData(tsFile);
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
      logger.error(StorageEngineMessages.TSFILE_MISSING_SKIP_RECOVERY, tsFile);
      return;
    }

    writer =
        reinitialize
            ? reinitializeWriter(tsFile)
            : new RestorableTsFileIOWriter(
                tsFile,
                EncryptDBUtils.getFirstEncryptParamFromTSFilePath(tsFile.getAbsolutePath()));
    if (writer.hasCrashed()) {
      byte versionNumber;
      try (TsFileSequenceReader sequenceReader =
          new TsFileSequenceReader(
              tsFile.getAbsolutePath(),
              EncryptDBUtils.getFirstEncryptParamFromTSFilePath(tsFile.getAbsolutePath()))) {
        versionNumber = sequenceReader.readVersionNumber();
      } catch (NotCompatibleTsFileException e) {
        versionNumber = -1;
      }

      if (versionNumber != TSFileConfig.VERSION_NUMBER) {
        // cannot rewrite a file with V3 header, delete it first
        writer.close();
        org.apache.iotdb.commons.utils.FileUtils.deleteFileIfExist(tsFile);
        writer =
            EncryptDBUtils.createRestorableWriterForNewFile(
                tsFile,
                tsFileResource.getDatabaseName(),
                EncryptDBUtils.getFirstEncryptParamFromTSFilePath(tsFile.getAbsolutePath()));
      }
    } else {
      reconstructResourceFile();
    }
  }

  private static boolean hasNoPersistedData(File file) throws IOException {
    byte[] magic = TSFileConfig.MAGIC_STRING.getBytes(StandardCharsets.UTF_8);
    try (FileChannel input = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
      if (input.size() == 0) {
        return true;
      }
      if (input.size() != magic.length + Byte.BYTES) {
        return false;
      }
      byte[] expected = Arrays.copyOf(magic, magic.length + Byte.BYTES);
      expected[magic.length] = TSFileConfig.VERSION_NUMBER;
      ByteBuffer header = ByteBuffer.allocate(expected.length);
      IOUtils.readFully(input, header);
      return Arrays.equals(expected, header.array());
    }
  }

  private RestorableTsFileIOWriter reinitializeWriter(File file) throws IOException {
    // Resolve the new-file policy before modifying even a header-only file.
    EncryptParameter parameter =
        EncryptDBUtils.createEncryptParameterForNewFile(
            file,
            tsFileResource.getDatabaseName(),
            EncryptDBUtils.getFirstEncryptParamFromTSFilePath(file.getAbsolutePath()));
    try {
      try (FileChannel output = FileChannel.open(file.toPath(), StandardOpenOption.WRITE)) {
        output.truncate(0);
      }
      return new RestorableTsFileIOWriter(file, parameter);
    } finally {
      if (parameter != null && parameter.isTdePageAead()) {
        parameter.close();
      }
    }
  }

  private void loadResourceFile() throws IOException {
    try {
      tsFileResource.deserialize();
    } catch (Throwable e) {
      logger.warn(
          StorageEngineMessages
              .STORAGE_LOG_CANNOT_DESERIALIZE_RESOURCE_FILE_OF_TRY_TO_RECONSTRUCT_IT_F82299C6,
          tsFileResource.getTsFile(),
          e);
      reconstructResourceFile();
    }
  }

  protected void reconstructResourceFile() throws IOException {
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(
            tsFileResource.getTsFile().getAbsolutePath(),
            EncryptDBUtils.getFirstEncryptParamFromTSFilePath(
                tsFileResource.getTsFile().getAbsolutePath()))) {
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
