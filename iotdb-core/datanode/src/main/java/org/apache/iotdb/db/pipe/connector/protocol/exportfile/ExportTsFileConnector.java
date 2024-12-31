/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.connector.protocol.exportfile;

import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
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
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;

import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_EXPORT_TSFILE_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_EXPORT_TSFILE_PATH_KEY;

public class ExportTsFileConnector implements PipeConnector {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExportTsFileConnector.class);

  private File exportPath;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    final PipeParameters parameters = validator.getParameters();
    validator.validate(
        args -> (boolean) args[0] || (boolean) args[1],
        String.format(
            "One of %s and %s must be specified",
            CONNECTOR_EXPORT_TSFILE_PATH_KEY, SINK_EXPORT_TSFILE_PATH_KEY),
        parameters.hasAttribute(CONNECTOR_EXPORT_TSFILE_PATH_KEY),
        parameters.hasAttribute(SINK_EXPORT_TSFILE_PATH_KEY));
  }

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    if (parameters.hasAttribute(SINK_EXPORT_TSFILE_PATH_KEY)) {
      exportPath = new File(parameters.getString(SINK_EXPORT_TSFILE_PATH_KEY));
    } else if (parameters.hasAttribute(CONNECTOR_EXPORT_TSFILE_PATH_KEY)) {
      exportPath = new File(parameters.getString(CONNECTOR_EXPORT_TSFILE_PATH_KEY));
    } else {
      // This should not happen
      throw new PipeException("Export TsFile path not found in parameters");
    }
  }

  @Override
  public void handshake() throws Exception {
    if (exportPath == null) {
      throw new PipeException("Handshake should not take place before customize");
    }

    if (!exportPath.exists() && !exportPath.mkdirs()) {
      throw new PipeException("Export TsFile path not exist and can't be created");
    }

    if (!exportPath.isDirectory()) {
      throw new PipeException("Export TsFile path already exists and is not a directory");
    }
  }

  @Override
  public void heartbeat() throws Exception {
    handshake();
  }

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    LOGGER.warn(
        "TabletInsertionEvent is not supported, will skip this event: {}", tabletInsertionEvent);
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    if (!(tsFileInsertionEvent instanceof PipeTsFileInsertionEvent)) {
      LOGGER.warn(
          "{} only supports PipeTsFileInsertionEvent", ExportTsFileConnector.class.getName());
      return;
    }

    final PipeTsFileInsertionEvent event = (PipeTsFileInsertionEvent) tsFileInsertionEvent;
    final File sourceTsFile = event.getTsFile();
    final File targetPath =
        new File(
            exportPath.getPath()
                + File.separator
                + event.getPipeNameWithCreationTime()
                + File.separator
                + sourceTsFile.getName());
    if (targetPath.exists()) {
      LOGGER.info(
          "File already exists when exporting tsfile, will skip this file. Origin: {}, Target: {}",
          sourceTsFile.getAbsolutePath(),
          targetPath.getAbsolutePath());
      return;
    }
    if (!targetPath.getParentFile().exists() && !targetPath.getParentFile().mkdirs()) {
      LOGGER.warn(
          "Parent directory creation failed when exporting tsfile. Origin: {}, Target: {}",
          sourceTsFile.getAbsolutePath(),
          targetPath.getAbsolutePath());
      return;
    }

    if (!event.increaseReferenceCount(ExportTsFileConnector.class.getName())) {
      return;
    }
    try {
      Files.copy(sourceTsFile.toPath(), targetPath.toPath());
    } catch (final FileAlreadyExistsException e) {
      LOGGER.info(
          "File already exists when exporting tsfile, will skip this file. Origin: {}, Target: {}",
          sourceTsFile.getAbsolutePath(),
          targetPath.getAbsolutePath());
    } catch (final IOException e) {
      LOGGER.warn(
          "File copy failed when exporting tsfile. Origin: {}, Target: {}",
          sourceTsFile.getAbsolutePath(),
          targetPath.getAbsolutePath(),
          e);
    } finally {
      event.decreaseReferenceCount(ExportTsFileConnector.class.getName(), false);
    }
  }

  @Override
  public void transfer(Event event) throws Exception {
    // Do nothing for generic events
  }

  @Override
  public void close() throws Exception {
    // Do nothing
  }
}
