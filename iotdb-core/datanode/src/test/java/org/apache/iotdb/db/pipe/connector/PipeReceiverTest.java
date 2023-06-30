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

package org.apache.iotdb.db.pipe.connector;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.pipe.connector.v1.IoTDBThriftReceiverV1;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferHandshakeReq;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferTabletReq;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.mockito.Mockito.mock;

public class PipeReceiverTest {
  @Test
  public void testIoTDBThriftReceiverV1() {
    IoTDBThriftReceiverV1 receiver = new IoTDBThriftReceiverV1();
    try {
      receiver.receive(
          PipeTransferHandshakeReq.toTPipeTransferReq(
              CommonDescriptor.getInstance().getConfig().getTimestampPrecision()),
          mock(IPartitionFetcher.class),
          mock(ISchemaFetcher.class));
      receiver.receive(
          PipeTransferTabletReq.toTPipeTransferReq(
              new Tablet(
                  "root.sg.d",
                  Collections.singletonList(new MeasurementSchema("s", TSDataType.INT32))),
              true),
          mock(IPartitionFetcher.class),
          mock(ISchemaFetcher.class));
    } catch (IOException e) {
      Assert.fail();
    }
  }
}
