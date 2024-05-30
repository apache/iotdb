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

package org.apache.iotdb.confignode.procedure.impl.schema;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class SetTTLProcedureTest {

  @Test
  public void serializeDeserializeTest() throws IOException, IllegalPathException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    // test1
    PartialPath path = new PartialPath("root.test.sg1.group1.group1.**");
    SetTTLPlan setTTLPlan = new SetTTLPlan(Arrays.asList(path.getNodes()), 1928300234200L);
    SetTTLProcedure proc = new SetTTLProcedure(setTTLPlan, false);

    proc.serialize(outputStream);
    ByteBuffer buffer =
        ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    SetTTLProcedure proc2 = (SetTTLProcedure) ProcedureFactory.getInstance().create(buffer);
    Assert.assertTrue(proc.equals(proc2));
    buffer.clear();
    byteArrayOutputStream.reset();

    // test2
    path = new PartialPath("root.**");
    setTTLPlan = new SetTTLPlan(Arrays.asList(path.getNodes()), -1);
    proc = new SetTTLProcedure(setTTLPlan, false);

    proc.serialize(outputStream);
    buffer = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    proc2 = (SetTTLProcedure) ProcedureFactory.getInstance().create(buffer);
    Assert.assertTrue(proc.equals(proc2));
    buffer.clear();
    byteArrayOutputStream.reset();
  }
}
