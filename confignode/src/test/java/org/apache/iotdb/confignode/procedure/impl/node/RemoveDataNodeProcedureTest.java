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
package org.apache.iotdb.confignode.procedure.impl.node;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class RemoveDataNodeProcedureTest {

  @Test
  public void serDeTest() throws IOException {
    RemoveDataNodeProcedure procedure0 =
        new RemoveDataNodeProcedure(
            new TDataNodeLocation(
                10,
                new TEndPoint("127.0.0.1", 6667),
                new TEndPoint("127.0.0.1", 6668),
                new TEndPoint("127.0.0.1", 6669),
                new TEndPoint("127.0.0.1", 6670),
                new TEndPoint("127.0.0.1", 6671)));

    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      procedure0.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      Assert.assertEquals(procedure0, ProcedureFactory.getInstance().create(buffer));
    }
  }
}
