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

package org.apache.iotdb.db.pipe.processor.schemachange;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.queryengine.plan.execution.config.TableConfigTaskVisitor;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MAX_DATABASE_NAME_LENGTH;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_RENAME_DATABASE_NEW_DB_NAME;
import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class RenameDatabaseProcessor implements PipeProcessor {

  // Currently this processor is only used for table model.
  // For tree model events, this processor will simply ignore them
  private String newDatabaseName;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    validator.validateRequiredAttribute(PROCESSOR_RENAME_DATABASE_NEW_DB_NAME);
    newDatabaseName = validator.getParameters().getString(PROCESSOR_RENAME_DATABASE_NEW_DB_NAME);
    try {
      TableConfigTaskVisitor.validateDatabaseName(newDatabaseName);
    } catch (final Exception e) {
      throw new PipeException(
          String.format(
              "The new database name %s is invalid, it should not contain '%s', "
                  + "should match the pattern %s, and the length should not exceed %d",
              newDatabaseName,
              PATH_SEPARATOR,
              IoTDBConfig.STORAGE_GROUP_PATTERN,
              MAX_DATABASE_NAME_LENGTH),
          e);
    }
  }

  @Override
  public void customize(PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration)
      throws Exception {
    // Do nothing
  }

  @Override
  public void process(TabletInsertionEvent tabletInsertionEvent, EventCollector eventCollector)
      throws Exception {
    renameDatabase(tabletInsertionEvent, eventCollector);
  }

  @Override
  public void process(TsFileInsertionEvent tsFileInsertionEvent, EventCollector eventCollector)
      throws Exception {
    renameDatabase(tsFileInsertionEvent, eventCollector);
  }

  @Override
  public void process(Event event, EventCollector eventCollector) throws Exception {
    renameDatabase(event, eventCollector);
  }

  private void renameDatabase(final Event event, final EventCollector eventCollector)
      throws Exception {
    // This processor is only used for table model insertion events
    if (event instanceof PipeInsertionEvent && ((PipeInsertionEvent) event).isTableModelEvent()) {
      ((PipeInsertionEvent) event).renameTableModelDatabase(newDatabaseName);
    }

    eventCollector.collect(event);
  }

  @Override
  public void close() throws Exception {
    // Do nothing
  }
}
