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

package org.apache.iotdb.db.queryengine.execution.operator.schema.source;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.table.Audit;
import org.apache.iotdb.db.schemaengine.rescon.ISchemaRegionStatistics;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.ITimeSeriesSchemaInfo;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TimeSeriesSchemaSourceTest {

  @Test
  public void testCountSourceSkipsImplicitInternalDatabases() throws Exception {
    final ISchemaSource<ITimeSeriesSchemaInfo> countSource =
        SchemaSourceFactory.getTimeSeriesSchemaCountSource(
            new PartialPath("root.**"),
            false,
            null,
            Collections.emptyMap(),
            SchemaConstant.ALL_MATCH_SCOPE);

    assertTrue(countSource.shouldSkipSchemaRegion(mockSchemaRegion(SchemaConstant.SYSTEM_DATABASE)));
    assertTrue(countSource.shouldSkipSchemaRegion(mockSchemaRegion(SchemaConstant.AUDIT_DATABASE)));
    assertTrue(countSource.shouldSkipSchemaRegion(mockSchemaRegion(Audit.TABLE_MODEL_AUDIT_DATABASE)));
    assertFalse(countSource.shouldSkipSchemaRegion(mockSchemaRegion("root.sg")));
  }

  @Test
  public void testCountSourceKeepsExplicitInternalDatabaseQueries() throws Exception {
    final ISchemaSource<ITimeSeriesSchemaInfo> systemCountSource =
        SchemaSourceFactory.getTimeSeriesSchemaCountSource(
            new PartialPath("root.__system.**"),
            false,
            null,
            Collections.emptyMap(),
            SchemaConstant.ALL_MATCH_SCOPE);
    assertFalse(
        systemCountSource.shouldSkipSchemaRegion(mockSchemaRegion(SchemaConstant.SYSTEM_DATABASE)));
    assertTrue(
        systemCountSource.shouldSkipSchemaRegion(mockSchemaRegion(SchemaConstant.AUDIT_DATABASE)));

    final ISchemaSource<ITimeSeriesSchemaInfo> auditCountSource =
        SchemaSourceFactory.getTimeSeriesSchemaCountSource(
            new PartialPath("root.__audit.**"),
            false,
            null,
            Collections.emptyMap(),
            SchemaConstant.ALL_MATCH_SCOPE);
    assertFalse(
        auditCountSource.shouldSkipSchemaRegion(mockSchemaRegion(SchemaConstant.AUDIT_DATABASE)));
    assertTrue(
        auditCountSource.shouldSkipSchemaRegion(mockSchemaRegion(SchemaConstant.SYSTEM_DATABASE)));
  }

  @Test
  public void testCountSourceSkipsWildcardSecondNodeForInternalDatabases() throws Exception {
    final ISchemaSource<ITimeSeriesSchemaInfo> countSource =
        SchemaSourceFactory.getTimeSeriesSchemaCountSource(
            new PartialPath("root.*.**"),
            false,
            null,
            Collections.emptyMap(),
            SchemaConstant.ALL_MATCH_SCOPE);

    assertTrue(countSource.shouldSkipSchemaRegion(mockSchemaRegion(SchemaConstant.SYSTEM_DATABASE)));
    assertTrue(countSource.shouldSkipSchemaRegion(mockSchemaRegion(SchemaConstant.AUDIT_DATABASE)));
    assertFalse(countSource.shouldSkipSchemaRegion(mockSchemaRegion("root.sg")));
  }

  @Test
  public void testCountSourceKeepsExactInternalDatabaseQueries() throws Exception {
    final ISchemaSource<ITimeSeriesSchemaInfo> systemCountSource =
        SchemaSourceFactory.getTimeSeriesSchemaCountSource(
            new PartialPath("root.__system"),
            false,
            null,
            Collections.emptyMap(),
            SchemaConstant.ALL_MATCH_SCOPE);
    assertFalse(
        systemCountSource.shouldSkipSchemaRegion(mockSchemaRegion(SchemaConstant.SYSTEM_DATABASE)));

    final ISchemaSource<ITimeSeriesSchemaInfo> auditCountSource =
        SchemaSourceFactory.getTimeSeriesSchemaCountSource(
            new PartialPath("root.__audit"),
            false,
            null,
            Collections.emptyMap(),
            SchemaConstant.ALL_MATCH_SCOPE);
    assertFalse(
        auditCountSource.shouldSkipSchemaRegion(mockSchemaRegion(SchemaConstant.AUDIT_DATABASE)));
  }

  @Test
  public void testShowSourceDoesNotSkipInternalDatabases() throws Exception {
    final ISchemaSource<ITimeSeriesSchemaInfo> showSource =
        SchemaSourceFactory.getTimeSeriesSchemaScanSource(
            new PartialPath("root.**"),
            false,
            0,
            0,
            null,
            Collections.emptyMap(),
            SchemaConstant.ALL_MATCH_SCOPE,
            null);

    assertFalse(showSource.shouldSkipSchemaRegion(mockSchemaRegion(SchemaConstant.SYSTEM_DATABASE)));
    assertFalse(showSource.shouldSkipSchemaRegion(mockSchemaRegion(SchemaConstant.AUDIT_DATABASE)));
  }

  @Test
  public void testCountStatisticIncludesView() throws Exception {
    final ISchemaSource<ITimeSeriesSchemaInfo> countSource =
        SchemaSourceFactory.getTimeSeriesSchemaCountSource(
            new PartialPath("root.sg.**"),
            false,
            null,
            Collections.emptyMap(),
            SchemaConstant.ALL_MATCH_SCOPE);
    final ISchemaRegion schemaRegion = mockSchemaRegion("root.sg");
    final ISchemaRegionStatistics schemaRegionStatistics =
        Mockito.mock(ISchemaRegionStatistics.class);

    Mockito.when(schemaRegion.getSchemaRegionStatistics()).thenReturn(schemaRegionStatistics);
    Mockito.when(schemaRegionStatistics.getSeriesNumber(true)).thenReturn(5L);

    assertEquals(5L, countSource.getSchemaStatistic(schemaRegion));
    Mockito.verify(schemaRegionStatistics).getSeriesNumber(true);
    Mockito.verify(schemaRegionStatistics, Mockito.never()).getSeriesNumber(false);
  }

  private ISchemaRegion mockSchemaRegion(final String database) {
    final ISchemaRegion schemaRegion = Mockito.mock(ISchemaRegion.class);
    Mockito.when(schemaRegion.getDatabaseFullPath()).thenReturn(database);
    return schemaRegion;
  }
}
