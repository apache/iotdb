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
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MAX_DATABASE_NAME_LENGTH;
import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_RENAME_DATABASE_NAME;
import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class RenameDatabaseProcessor implements PipeProcessor {

  private String treeModelDataBaseName;
  private String tableModelDataBaseName;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    final String dataBaseName = validator.getParameters().getString(PROCESSOR_RENAME_DATABASE_NAME);
    validator.validate(
        dataBase -> !(dataBase == null || isDatabaseNameInvalid((String) dataBase)),
        String.format(
            "Database name validation failed. The provided database name: %s", dataBaseName),
        dataBaseName);
    tableModelDataBaseName = dataBaseName;
    treeModelDataBaseName = PATH_ROOT + PATH_SEPARATOR + dataBaseName;
  }

  @Override
  public void customize(PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration)
      throws Exception {}

  @Override
  public void process(TabletInsertionEvent tabletInsertionEvent, EventCollector eventCollector)
      throws Exception {
    resetDataBaseName(tabletInsertionEvent, eventCollector);
  }

  @Override
  public void process(TsFileInsertionEvent tsFileInsertionEvent, EventCollector eventCollector)
      throws Exception {
    resetDataBaseName(tsFileInsertionEvent, eventCollector);
  }

  @Override
  public void process(Event event, EventCollector eventCollector) throws Exception {
    resetDataBaseName(event, eventCollector);
  }

  @Override
  public void close() throws Exception {}

  private void resetDataBaseName(final Event event, final EventCollector eventCollector)
      throws Exception {
    if (!((event instanceof PipeInsertionEvent)
        && ((PipeInsertionEvent) event).isTableModelEvent())) {
      eventCollector.collect(event);
      return;
    }
    final PipeInsertionEvent pipeInsertionEvent = (PipeInsertionEvent) event;
    // We don't need lazy loading here, because TableModeDataBaseName is generated, all databaseName
    // just reference the same variable, no additional memory
    pipeInsertionEvent.setTreeModelDatabaseNameAndTableModelDataBase(
        treeModelDataBaseName, tableModelDataBaseName);
    eventCollector.collect(pipeInsertionEvent);
  }

  private boolean isDatabaseNameInvalid(final String dataBaseName) {
    return dataBaseName.contains(PATH_SEPARATOR)
        || !IoTDBConfig.STORAGE_GROUP_PATTERN.matcher(dataBaseName).matches()
        || dataBaseName.length() > MAX_DATABASE_NAME_LENGTH;
  }
}
