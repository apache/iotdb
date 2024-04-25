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
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.ConstantViewOperand;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

public class AlterLogicalViewProcedureTest {
  @Test
  public void serializeDeserializeTest() throws IllegalPathException, IOException {
    AlterLogicalViewProcedure alterLogicalViewProcedure =
        new AlterLogicalViewProcedure(
            "1",
            new HashMap<PartialPath, ViewExpression>() {
              {
                put(
                    new PartialPath("root.sg"),
                    new ConstantViewOperand(TSDataType.BOOLEAN, "true"));
              }
            },
            false);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    alterLogicalViewProcedure.serialize(dataOutputStream);

    ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.ALTER_LOGICAL_VIEW_PROCEDURE.getTypeCode(), byteBuffer.getShort());

    AlterLogicalViewProcedure deserializedProcedure = new AlterLogicalViewProcedure(false);
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(alterLogicalViewProcedure.getQueryId(), deserializedProcedure.getQueryId());
    // Currently skip the "equals" method since "equals" of ViewExpression is not implemented
  }
}
