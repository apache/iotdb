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

package org.apache.iotdb.confignode.consensus.response;

import org.apache.iotdb.common.rpc.thrift.Model;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.commons.udf.UDFInformation;
import org.apache.iotdb.confignode.consensus.response.function.FunctionTableResp;
import org.apache.iotdb.confignode.consensus.response.trigger.TransferringTriggersResp;
import org.apache.iotdb.confignode.consensus.response.trigger.TriggerLocationResp;
import org.apache.iotdb.confignode.consensus.response.trigger.TriggerTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetLocationForTriggerResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTriggerTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetUDFTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TTriggerState;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.trigger.api.enums.FailureStrategy;
import org.apache.iotdb.trigger.api.enums.TriggerEvent;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ConvertToThriftRespTest {
  @Test
  public void convertFunctionRespTest() throws IOException {
    FunctionTableResp functionTableResp =
        new FunctionTableResp(
            new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
            ImmutableList.of(
                new UDFInformation("test1", "test1", Model.TREE, true, true, "test1.jar", "12345"),
                new UDFInformation(
                    "test2", "test2", Model.TREE, true, true, "test2.jar", "12342")));
    TGetUDFTableResp tGetUDFTableResp = functionTableResp.convertToThriftResponse();
    Assert.assertEquals(functionTableResp.getStatus(), tGetUDFTableResp.status);
    Assert.assertEquals(
        functionTableResp.getUdfInformation().get(0),
        UDFInformation.deserialize(tGetUDFTableResp.allUDFInformation.get(0)));
    Assert.assertEquals(
        functionTableResp.getUdfInformation().get(1),
        UDFInformation.deserialize(tGetUDFTableResp.allUDFInformation.get(1)));
  }

  @Test
  public void convertTriggerRespTest() throws IOException, IllegalPathException {
    TransferringTriggersResp transferringTriggersResp =
        new TransferringTriggersResp(ImmutableList.of("123", "234"));
    Assert.assertEquals(
        ImmutableList.of("123", "234"), transferringTriggersResp.getTransferringTriggers());

    TriggerLocationResp triggerLocationResp =
        new TriggerLocationResp(
            new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
            new TDataNodeLocation(
                10000,
                new TEndPoint("127.0.0.1", 6600),
                new TEndPoint("127.0.0.1", 7700),
                new TEndPoint("127.0.0.1", 8800),
                new TEndPoint("127.0.0.1", 9900),
                new TEndPoint("127.0.0.1", 11000)));
    TGetLocationForTriggerResp tGetLocationForTriggerResp =
        triggerLocationResp.convertToThriftResponse();
    Assert.assertEquals(triggerLocationResp.getStatus(), tGetLocationForTriggerResp.status);
    Assert.assertEquals(
        triggerLocationResp.getDataNodeLocation(), tGetLocationForTriggerResp.dataNodeLocation);

    TriggerTableResp triggerTableResp =
        new TriggerTableResp(
            new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
            ImmutableList.of(
                new TriggerInformation(
                    new PartialPath("root.test.**"),
                    "test1",
                    "test1.class",
                    true,
                    "test1.jar",
                    null,
                    TriggerEvent.AFTER_INSERT,
                    TTriggerState.INACTIVE,
                    false,
                    null,
                    FailureStrategy.OPTIMISTIC,
                    "testMD5test")));
    TGetTriggerTableResp tGetTriggerTableResp = triggerTableResp.convertToThriftResponse();
    Assert.assertEquals(triggerTableResp.getStatus(), tGetTriggerTableResp.status);
    Assert.assertEquals(
        triggerTableResp.getAllTriggerInformation().get(0),
        TriggerInformation.deserialize(tGetTriggerTableResp.allTriggerInformation.get(0)));
  }

  @Test
  public void convertJarRespTest() throws IOException {
    FunctionTableResp functionTableResp =
        new FunctionTableResp(
            new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
            ImmutableList.of(
                new UDFInformation("test1", "test1", Model.TREE, true, true, "test1.jar", "12345"),
                new UDFInformation(
                    "test2", "test2", Model.TREE, true, true, "test2.jar", "12342")));
    TGetUDFTableResp tGetUDFTableResp = functionTableResp.convertToThriftResponse();
    Assert.assertEquals(functionTableResp.getStatus(), tGetUDFTableResp.status);
    Assert.assertEquals(
        functionTableResp.getUdfInformation().get(0),
        UDFInformation.deserialize(tGetUDFTableResp.allUDFInformation.get(0)));
    Assert.assertEquals(
        functionTableResp.getUdfInformation().get(1),
        UDFInformation.deserialize(tGetUDFTableResp.allUDFInformation.get(1)));
  }
}
