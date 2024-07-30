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

package org.apache.iotdb.confignode.procedure.impl.schema.table;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

public class SetTablePropertiesProcedureTest {
  @Test
  public void serializeDeserializeTest() throws IllegalPathException, IOException {
    final SetTablePropertiesProcedure setTablePropertiesProcedure =
        new SetTablePropertiesProcedure(
            "root.database1",
            "table1",
            "0",
            new HashMap<String, String>() {
              {
                put("prop1", "value1");
                put("ttl", null);
              }
            });

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    setTablePropertiesProcedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.SET_TABLE_PROPERTIES_PROCEDURE.getTypeCode(), byteBuffer.getShort());

    final SetTablePropertiesProcedure deserializedProcedure = new SetTablePropertiesProcedure();
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(setTablePropertiesProcedure, deserializedProcedure);
  }
}
