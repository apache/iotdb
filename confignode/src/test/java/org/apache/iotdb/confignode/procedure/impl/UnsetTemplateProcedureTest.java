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

package org.apache.iotdb.confignode.procedure.impl;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.procedure.impl.schema.UnsetTemplateProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class UnsetTemplateProcedureTest {

  @Test
  public void serializeDeserializeTest() throws IllegalPathException, IOException {
    String queryId = "1";
    Template template = new Template();
    template.setId(0);
    template.setName("t1");
    template.addMeasurements(
        new String[] {"s1", "s2"},
        new TSDataType[] {TSDataType.INT32, TSDataType.FLOAT},
        new TSEncoding[] {TSEncoding.PLAIN, TSEncoding.BITMAP},
        new CompressionType[] {CompressionType.UNCOMPRESSED, CompressionType.GZIP});
    PartialPath path = new PartialPath("root.sg");
    UnsetTemplateProcedure unsetTemplateProcedure =
        new UnsetTemplateProcedure(queryId, template, path);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    unsetTemplateProcedure.serialize(dataOutputStream);

    ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.UNSET_TEMPLATE_PROCEDURE.getTypeCode(), byteBuffer.getShort());

    UnsetTemplateProcedure deserializedProcedure = new UnsetTemplateProcedure();
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(queryId, deserializedProcedure.getQueryId());
    Assert.assertEquals(template.getId(), deserializedProcedure.getTemplateId());
    Assert.assertEquals(template.getName(), deserializedProcedure.getTemplateName());
    Assert.assertEquals(
        template.getSchemaMap(), deserializedProcedure.getTemplate().getSchemaMap());
    Assert.assertEquals(path, deserializedProcedure.getPath());
  }
}
