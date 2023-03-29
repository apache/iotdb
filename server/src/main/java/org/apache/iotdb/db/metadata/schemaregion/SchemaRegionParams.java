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

package org.apache.iotdb.db.metadata.schemaregion;

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.rescon.ISchemaEngineStatistics;
import org.apache.iotdb.external.api.ISeriesNumerMonitor;

public class SchemaRegionParams implements ISchemaRegionParams {

  private final PartialPath database;

  private final SchemaRegionId schemaRegionId;

  private final ISchemaEngineStatistics schemaEngineStatistics;

  private final ISeriesNumerMonitor seriesNumberMonitor;

  public SchemaRegionParams(
      PartialPath database,
      SchemaRegionId schemaRegionId,
      ISchemaEngineStatistics schemaEngineStatistics,
      ISeriesNumerMonitor seriesNumberMonitor) {
    this.database = database;
    this.schemaRegionId = schemaRegionId;
    this.schemaEngineStatistics = schemaEngineStatistics;
    this.seriesNumberMonitor = seriesNumberMonitor;
  }

  @Override
  public PartialPath getDatabase() {
    return database;
  }

  @Override
  public SchemaRegionId getSchemaRegionId() {
    return schemaRegionId;
  }

  @Override
  public ISchemaEngineStatistics getSchemaEngineStatistics() {
    return schemaEngineStatistics;
  }

  @Override
  public ISeriesNumerMonitor getSeriesNumberMonitor() {
    return seriesNumberMonitor;
  }
}
