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

package org.apache.iotdb.db.schemaengine.template;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.protocol.thrift.OperationType;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.CreateSchemaTemplateStatement;
import org.apache.iotdb.db.utils.ErrorHandlingUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ConfigNodeClientManager.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
public class ClusterTemplateManagerTest {

  @SuppressWarnings("unchecked")
  private static final IClientManager<ConfigRegionId, ConfigNodeClient> CLIENT_MANAGER =
      Mockito.mock(IClientManager.class);

  private ConfigNodeClient client;

  @Before
  public void setUp() throws Exception {
    Mockito.reset(CLIENT_MANAGER);
    client = Mockito.mock(ConfigNodeClient.class);
    PowerMockito.mockStatic(ConfigNodeClientManager.class);
    PowerMockito.when(ConfigNodeClientManager.getInstance()).thenReturn(CLIENT_MANAGER);
    Mockito.when(CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)).thenReturn(client);
  }

  @Test
  public void testGetTemplateRpcFailure() throws Exception {
    TException rpcFailure = new TException("connection refused");
    Mockito.when(client.getTemplate("test_template")).thenThrow(rpcFailure);

    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> ClusterTemplateManager.getInstance().getTemplate("test_template"));

    TSStatus status = assertRpcFailure(exception, rpcFailure, TSStatusCode.UNDEFINED_TEMPLATE);
    assertTrue(status.getMessage().contains("test_template"));
  }

  @Test
  public void testGetAllTemplatesRpcFailure() throws Exception {
    TException rpcFailure = new TException("connection refused");
    Mockito.when(client.getAllTemplates()).thenThrow(rpcFailure);

    RuntimeException exception =
        assertThrows(
            RuntimeException.class, () -> ClusterTemplateManager.getInstance().getAllTemplates());

    assertRpcFailure(exception, rpcFailure, TSStatusCode.UNDEFINED_TEMPLATE);
  }

  @Test
  public void testCreateTemplateRpcFailure() throws Exception {
    TException rpcFailure = new TException("connection refused");
    Mockito.when(client.createSchemaTemplate(Mockito.any())).thenThrow(rpcFailure);
    CreateSchemaTemplateStatement statement =
        new CreateSchemaTemplateStatement(
            "test_template",
            Collections.singletonList(Collections.singletonList("s")),
            Collections.singletonList(Collections.singletonList(TSDataType.INT32)),
            Collections.singletonList(Collections.singletonList(TSEncoding.PLAIN)),
            Collections.singletonList(Collections.singletonList(CompressionType.UNCOMPRESSED)),
            false);

    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> ClusterTemplateManager.getInstance().createSchemaTemplate(statement));

    TSStatus status = assertRpcFailure(exception, rpcFailure, TSStatusCode.CREATE_TEMPLATE_ERROR);
    assertTrue(status.getMessage().contains("test_template"));
  }

  private static TSStatus assertRpcFailure(
      RuntimeException exception, TException rpcFailure, TSStatusCode statusCode) {
    TSStatus status =
        ErrorHandlingUtils.onQueryException(exception, OperationType.EXECUTE_STATEMENT);
    assertEquals(statusCode.getStatusCode(), status.getCode());
    assertTrue(status.getMessage().contains("ConfigNode"));
    assertTrue(status.getMessage().contains("connection refused"));
    assertEquals(1, exception.getCause().getSuppressed().length);
    assertSame(rpcFailure, exception.getCause().getSuppressed()[0]);
    return status;
  }
}
