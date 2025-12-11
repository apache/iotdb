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

package org.apache.iotdb.db.pipe.sink.protocol.tsfile;

import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * TsFileSink - Export Pipe data as TSFile format
 *
 * <p>Supports two modes:
 *
 * <ul>
 *   <li>Local mode (local): Write directly to local directory
 *   <li>SCP mode (scp): Write to local temporary directory first, then transfer to remote server
 *       via SCP
 * </ul>
 *
 * <p>Supports two event types:
 *
 * <ul>
 *   <li>TabletInsertionEvent: Build new TSFile, write InsertNode data
 *   <li>TsFileInsertionEvent: Copy existing TSFile to target directory
 * </ul>
 *
 * <p>Object file handling:
 *
 * <ul>
 *   <li>Scan Object data paths in events
 *   <li>Hard link Object files to ${tsfilename}/ subdirectory in target directory
 *   <li>Maintain original relative path structure
 * </ul>
 */
public class TsFileSink implements PipeConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileSink.class);

  // Configuration parameter keys
  private static final String SINK_FILE_MODE_KEY = "sink.file-mode";
  private static final String SINK_TARGET_PATH_KEY = "sink.target-path";
  private static final String SINK_SCP_REMOTE_PATH_KEY = "sink.scp.remote-path";
  private static final String SINK_SCP_HOST_KEY = "sink.scp.host";
  private static final String SINK_SCP_PORT_KEY = "sink.scp.port";
  private static final String SINK_SCP_USER_KEY = "sink.scp.user";
  private static final String SINK_SCP_PASSWORD_KEY = "sink.scp.password";

  // Default values
  private static final String FILE_MODE_LOCAL = "local";
  private static final String FILE_MODE_SCP = "scp";
  private static final int DEFAULT_SCP_PORT = 22;

  // Runtime configuration
  private String fileMode;
  private String
      targetPath; // Target path for local mode, or temporary path for SCP mode (Writer's target
  // directory)

  // SCP configuration
  private String scpRemotePath;
  private String scpHost;
  private int scpPort;
  private String scpUser;
  private String scpPassword;

  // Runtime environment
  private String pipeName;
  private long creationTime;

  // TSFile builder
  private TsFileSinkWriter tsFileWriter;

  // File transfer
  private FileTransfer fileTransfer;

  // SCP file transfer (for type conversion)
  private ScpFileTransfer scpTransfer;

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    final PipeParameters parameters = validator.getParameters();

    // Validate file-mode
    final String mode =
        parameters
            .getStringOrDefault(Arrays.asList(SINK_FILE_MODE_KEY), FILE_MODE_LOCAL)
            .toLowerCase();

    if (!FILE_MODE_LOCAL.equals(mode) && !FILE_MODE_SCP.equals(mode)) {
      throw new PipeException(
          String.format("Invalid file-mode '%s'. Must be 'local' or 'scp'.", mode));
    }

    // Validate target path (required for both local and SCP modes)
    validator.validateRequiredAttribute(SINK_TARGET_PATH_KEY);

    // Validate SCP mode parameters
    if (FILE_MODE_SCP.equals(mode)) {
      validator.validateRequiredAttribute(SINK_SCP_REMOTE_PATH_KEY);
      validator.validateRequiredAttribute(SINK_SCP_HOST_KEY);
      validator.validateRequiredAttribute(SINK_SCP_USER_KEY);
      validator.validateRequiredAttribute(SINK_SCP_PASSWORD_KEY);
    }
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    this.pipeName = configuration.getRuntimeEnvironment().getPipeName();
    this.creationTime = configuration.getRuntimeEnvironment().getCreationTime();

    // Read configuration
    this.fileMode =
        parameters
            .getStringOrDefault(Arrays.asList(SINK_FILE_MODE_KEY), FILE_MODE_LOCAL)
            .toLowerCase();

    // Read target path (target path for local mode, or temporary path for SCP mode)
    this.targetPath = parameters.getString(SINK_TARGET_PATH_KEY);
    ensureDirectoryExists(targetPath);

    if (FILE_MODE_LOCAL.equals(fileMode)) {
      // Local mode
      LOGGER.info("TsFileSink configured in LOCAL mode, target path: {}", targetPath);
    } else {
      // SCP mode
      this.scpRemotePath = parameters.getString(SINK_SCP_REMOTE_PATH_KEY);
      this.scpHost = parameters.getString(SINK_SCP_HOST_KEY);
      this.scpPort = parameters.getIntOrDefault(Arrays.asList(SINK_SCP_PORT_KEY), DEFAULT_SCP_PORT);
      this.scpUser = parameters.getString(SINK_SCP_USER_KEY);
      this.scpPassword = parameters.getString(SINK_SCP_PASSWORD_KEY);

      LOGGER.info(
          "TsFileSink configured in SCP mode, local tmp: {}, remote: {}@{}:{}",
          targetPath,
          scpUser,
          scpHost,
          scpRemotePath);
    }

    // Initialize TSFile writer (use targetPath as target directory)
    this.tsFileWriter = new TsFileSinkWriter(targetPath, pipeName, creationTime);

    // Initialize file transfer
    if (FILE_MODE_LOCAL.equals(fileMode)) {
      // Local mode: use local file transfer
      this.fileTransfer = new LocalFileTransfer(targetPath);
    } else {
      // SCP mode: use SCP transfer
      this.scpTransfer = new ScpFileTransfer(scpHost, scpPort, scpUser, scpPassword, scpRemotePath);
      this.fileTransfer = scpTransfer;
    }
  }

  @Override
  public void handshake() throws Exception {
    // Test file transfer connection
    if (fileTransfer != null) {
      fileTransfer.testConnection();
      LOGGER.info("File transfer connection test successful");
    }
  }

  @Override
  public void heartbeat() throws Exception {
    // Check file transfer connection
    if (fileTransfer != null) {
      fileTransfer.testConnection();
    }
  }

  @Override
  public void transfer(final TabletInsertionEvent tabletInsertionEvent) throws Exception {
    // Handle TabletInsertionEvent: build new TSFile
    tsFileWriter.writeTablet(tabletInsertionEvent);

    // Check if flush is needed and transfer files
    final List<File> generatedFiles = tsFileWriter.flushTabletsIfNeeded();
    if (!generatedFiles.isEmpty() && fileTransfer != null) {
      transferFiles(generatedFiles);
    }
  }

  @Override
  public void transfer(final TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    // Handle TsFileInsertionEvent: copy TSFile
    final File copiedFile = tsFileWriter.copyTsFile(tsFileInsertionEvent);

    // Transfer files
    if (copiedFile != null && fileTransfer != null) {
      transferFiles(Collections.singletonList(copiedFile));
    }
  }

  /** Transfer files (including TSFile and related Object files) */
  private void transferFiles(final List<File> tsFiles) throws Exception {
    for (final File tsFile : tsFiles) {
      // Transfer TSFile
      fileTransfer.transferFile(tsFile);

      // Transfer Tablet Object directory (if exists)
      final File tabletObjectDir = new File(tsFileWriter.getTargetDirectory(), "tablet_objects");
      if (tabletObjectDir.exists() && tabletObjectDir.isDirectory()) {
        fileTransfer.transferDirectory(tabletObjectDir);
        LOGGER.info("Transferred tablet object directory");
      }

      // Transfer TsFile Object directory (if exists)
      final String tsFileName = tsFile.getName();
      final String tsFileNameWithoutSuffix = tsFileName.replace(".tsfile", "");
      final File tsFileObjectDir =
          new File(tsFileWriter.getTargetDirectory(), tsFileNameWithoutSuffix);

      if (tsFileObjectDir.exists() && tsFileObjectDir.isDirectory()) {
        fileTransfer.transferDirectory(tsFileObjectDir);
        LOGGER.info("Transferred tsfile object directory {}", tsFileObjectDir.getName());
      }
    }
  }

  @Override
  public void transfer(final Event event) throws Exception {
    if (event instanceof TabletInsertionEvent) {
      transfer((TabletInsertionEvent) event);
    } else if (event instanceof TsFileInsertionEvent) {
      transfer((TsFileInsertionEvent) event);
    } else {
      LOGGER.warn("Unsupported event type: {}", event.getClass().getName());
    }
  }

  @Override
  public void close() throws Exception {
    try {
      // Flush remaining Tablets and transfer
      if (tsFileWriter != null) {
        final List<File> remainingFiles = tsFileWriter.flushTablets();
        if (!remainingFiles.isEmpty() && fileTransfer != null) {
          transferFiles(remainingFiles);
        }
        tsFileWriter.close();
      }
    } finally {
      // Close file transfer
      if (fileTransfer != null) {
        fileTransfer.close();
      }
    }

    LOGGER.info("TsFileSink closed for pipe: {}", pipeName);
  }

  /** Ensure directory exists */
  private void ensureDirectoryExists(final String path) throws PipeException {
    final File dir = new File(path);
    if (!dir.exists()) {
      if (!dir.mkdirs()) {
        throw new PipeException("Failed to create directory: " + path);
      }
    }
    if (!dir.isDirectory()) {
      throw new PipeException("Path is not a directory: " + path);
    }
  }
}
