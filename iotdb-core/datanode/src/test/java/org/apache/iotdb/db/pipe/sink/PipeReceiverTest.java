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

package org.apache.iotdb.db.pipe.sink;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.pipe.receiver.protocol.thrift.IoTDBDataNodeReceiver;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferDataNodeHandshakeV1Req;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletRawReq;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

public class PipeReceiverTest {
  @Test
  public void testIoTDBThriftReceiverV1() {
    IoTDBDataNodeReceiver receiver = new IoTDBDataNodeReceiver();
    try {
      receiver.receive(
          PipeTransferDataNodeHandshakeV1Req.toTPipeTransferReq(
              CommonDescriptor.getInstance().getConfig().getTimestampPrecision()));
      receiver.receive(
          PipeTransferTabletRawReq.toTPipeTransferReq(
              new Tablet(
                  "root.sg.d",
                  Collections.singletonList(new MeasurementSchema("s", TSDataType.INT32))),
              true));
    } catch (IOException e) {
      Assert.fail();
    }
  }
}
