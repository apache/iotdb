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

package org.apache.iotdb.db.queryengine.plan.analyze.schema;

import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.common.schematree.IMeasurementSchemaInfo;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TreeDeviceSchemaCacheManager;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class TreeSchemaAutoCreationPolicyTest {

  private boolean originalAutoCreateSchema;
  private TreeDeviceSchemaCacheManager cache;
  private AutoCreateSchemaExecutor creator;
  private ClusterSchemaFetchExecutor fetcher;
  private ISchemaComputationWithAutoCreation computation;
  private ClusterSchemaTree fetchedSchema;
  private PartialPath device;

  @Before
  public void setUp() throws Exception {
    originalAutoCreateSchema =
        IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled();
    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(true);
    cache = mock(TreeDeviceSchemaCacheManager.class);
    creator = mock(AutoCreateSchemaExecutor.class);
    fetcher = mock(ClusterSchemaFetchExecutor.class);
    computation = mock(ISchemaComputationWithAutoCreation.class);
    device = new PartialPath("root.load_policy.d1");
    when(computation.getDevicePath()).thenReturn(device);
    when(computation.getMeasurements()).thenReturn(new String[] {"s1"});
    when(computation.getDataType(0)).thenReturn(TSDataType.INT64);
    when(cache.computeWithoutTemplate(computation)).thenReturn(Collections.singletonList(0));
    when(cache.computeWithTemplate(computation)).thenReturn(Collections.singletonList(0));
    when(cache.computeSourceOfLogicalView(computation))
        .thenReturn(new Pair<>(Collections.emptyList(), Collections.emptyList()));
    fetchedSchema = new ClusterSchemaTree();
    when(fetcher.fetchSchemaOfOneDevice(any(), any(), anyList(), any())).thenReturn(fetchedSchema);
    when(fetcher.fetchSchemaOfMultiDevices(anyList(), anyList(), anyList(), anyList(), any()))
        .thenReturn(fetchedSchema);
  }

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(originalAutoCreateSchema);
  }

  @Test
  public void testNormalSingleDeviceSchemaCreationPolicy() {
    final NormalSchemaFetcher schemaFetcher = new NormalSchemaFetcher(cache, creator, fetcher);
    for (final boolean allowed : new boolean[] {false, true, false}) {
      clearInvocations(creator);
      final MPPQueryContext context = context(allowed);
      Assert.assertEquals(
          Collections.singletonList(0),
          schemaFetcher.processNormalTimeSeries(computation, context));
      if (allowed) {
        verify(creator)
            .autoCreateTimeSeries(
                any(), eq(device), anyList(), any(), any(), eq(false), same(context));
      } else {
        verifyZeroInteractions(creator);
      }
    }
  }

  @Test
  public void testNormalMultiDeviceSchemaCreationPolicy() {
    final NormalSchemaFetcher schemaFetcher = new NormalSchemaFetcher(cache, creator, fetcher);
    for (final boolean allowed : new boolean[] {false, true, false}) {
      clearInvocations(creator, computation);
      final MPPQueryContext context = context(allowed);
      schemaFetcher.processNormalTimeSeries(Collections.singletonList(computation), context);
      if (allowed) {
        verify(creator)
            .autoCreateTimeSeries(
                any(),
                anyList(),
                anyList(),
                anyList(),
                anyList(),
                anyList(),
                anyList(),
                same(context));
      } else {
        verifyZeroInteractions(creator);
      }
      verify(computation).computeMeasurement(0, null);
    }
  }

  @Test
  public void testExistingSchemaRemainsUsableWhenCreationIsDisabled() throws Exception {
    fetchedSchema.appendSingleMeasurementPath(
        new MeasurementPath("root.load_policy.d1.s1", TSDataType.INT64));
    final NormalSchemaFetcher schemaFetcher = new NormalSchemaFetcher(cache, creator, fetcher);
    Assert.assertTrue(schemaFetcher.processNormalTimeSeries(computation, context(false)).isEmpty());
    verify(computation).computeMeasurement(eq(0), any(IMeasurementSchemaInfo.class));
    verifyZeroInteractions(creator);
  }

  @Test
  public void testTemplateSingleDeviceSchemaCreationPolicy() {
    final TemplateSchemaFetcher schemaFetcher = new TemplateSchemaFetcher(cache, creator, fetcher);
    final Template template = mock(Template.class);
    when(template.getName()).thenReturn("load_template");
    when(template.getId()).thenReturn(7);
    for (final boolean allowed : new boolean[] {false, true, false}) {
      clearInvocations(creator);
      final MPPQueryContext context = context(allowed);
      Assert.assertEquals(
          Collections.singletonList(0),
          schemaFetcher.processTemplateTimeSeries(
              new Pair<>(template, device), computation, context));
      if (allowed) {
        verify(creator)
            .autoExtendTemplate(eq("load_template"), anyList(), anyList(), same(context));
        verify(creator).autoActivateTemplate(any(), eq(device), eq(7), same(context));
      } else {
        verifyZeroInteractions(creator);
      }
    }
  }

  @Test
  public void testTemplateMultiDeviceSchemaCreationPolicy() {
    final TemplateSchemaFetcher schemaFetcher = new TemplateSchemaFetcher(cache, creator, fetcher);
    final Template template = mock(Template.class);
    when(template.getName()).thenReturn("load_template");
    final List<Pair<Template, PartialPath>> templateInfo =
        Collections.singletonList(new Pair<>(template, device));
    for (final boolean allowed : new boolean[] {false, true, false}) {
      clearInvocations(creator, computation);
      final MPPQueryContext context = context(allowed);
      schemaFetcher.processTemplateTimeSeries(
          templateInfo, Collections.singletonList(computation), context);
      if (allowed) {
        verify(creator).autoExtendTemplate(any(), same(context));
        verify(creator).autoActivateTemplate(any(), anyList(), anyList(), same(context));
      } else {
        verifyZeroInteractions(creator);
      }
      verify(computation).computeMeasurement(0, null);
    }
  }

  private static MPPQueryContext context(final boolean allowed) {
    final MPPQueryContext context = new MPPQueryContext(new QueryId("load_policy"));
    context.setSession(
        new SessionInfo(
            0,
            new UserEntity(0, "root", ""),
            ZoneOffset.UTC,
            IoTDBConstant.ClientVersion.V_1_0,
            null,
            SqlDialect.TREE,
            allowed));
    return context;
  }
}
