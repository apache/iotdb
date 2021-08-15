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

package org.apache.iotdb.cluster.server.handlers.caller;

import org.apache.iotdb.cluster.common.TestException;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PullMeasurementSchemaHandlerTest {

  @Test
  public void testComplete() throws InterruptedException {
    Node owner = TestUtils.getNode(1);
    String prefixPath = "root";
    AtomicReference<List<IMeasurementSchema>> result = new AtomicReference<>();
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      measurementSchemas.add(TestUtils.getTestMeasurementSchema(i));
    }

    PullMeasurementSchemaHandler handler =
        new PullMeasurementSchemaHandler(owner, Collections.singletonList(prefixPath), result);
    synchronized (result) {
      new Thread(
              () -> {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
                try {
                  dataOutputStream.writeInt(measurementSchemas.size());
                  for (IMeasurementSchema measurementSchema : measurementSchemas) {
                    measurementSchema.partialSerializeTo(dataOutputStream);
                  }
                } catch (IOException e) {
                  // ignore
                }
                PullSchemaResp resp = new PullSchemaResp();
                resp.setSchemaBytes(outputStream.toByteArray());
                handler.onComplete(resp);
              })
          .start();
      result.wait();
    }
    assertEquals(measurementSchemas, result.get());
  }

  @Test
  public void testError() throws InterruptedException {
    Node owner = TestUtils.getNode(1);
    String prefixPath = "root";
    AtomicReference<List<IMeasurementSchema>> result = new AtomicReference<>();

    PullMeasurementSchemaHandler handler =
        new PullMeasurementSchemaHandler(owner, Collections.singletonList(prefixPath), result);
    synchronized (result) {
      new Thread(() -> handler.onError(new TestException())).start();
      result.wait();
    }
    assertNull(result.get());
  }
}
