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
import org.apache.iotdb.confignode.procedure.impl.schema.DeactivateTemplateProcedure;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeactivateTemplateProcedureTest {

  @Test
  public void serializeDeserializeTest() throws IllegalPathException, IOException {
    String queryId = "1";
    Map<PartialPath, List<Template>> templateSetInfo = new HashMap<>();
    Template t1 = new Template();
    t1.setId(0);
    t1.setName("t1");
    t1.addMeasurements(
        new String[] {"s1", "s2"},
        new TSDataType[] {TSDataType.INT32, TSDataType.FLOAT},
        new TSEncoding[] {TSEncoding.PLAIN, TSEncoding.BITMAP},
        new CompressionType[] {CompressionType.UNCOMPRESSED, CompressionType.GZIP});

    Template t2 = new Template();
    t2.setId(0);
    t2.setName("t2");
    t2.addMeasurements(
        new String[] {"s3", "s4"},
        new TSDataType[] {TSDataType.FLOAT, TSDataType.INT32},
        new TSEncoding[] {TSEncoding.BITMAP, TSEncoding.PLAIN},
        new CompressionType[] {CompressionType.GZIP, CompressionType.UNCOMPRESSED});

    templateSetInfo.put(new PartialPath("root.sg1.**"), Arrays.asList(t1, t2));
    templateSetInfo.put(new PartialPath("root.sg2.**"), Arrays.asList(t2, t1));

    DeactivateTemplateProcedure deactivateTemplateProcedure =
        new DeactivateTemplateProcedure(queryId, templateSetInfo);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    deactivateTemplateProcedure.serialize(dataOutputStream);

    ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.DEACTIVATE_TEMPLATE_PROCEDURE.getTypeCode(), byteBuffer.getShort());

    DeactivateTemplateProcedure deserializedProcedure = new DeactivateTemplateProcedure();
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(queryId, deserializedProcedure.getQueryId());
    Assert.assertEquals(templateSetInfo, deserializedProcedure.getTemplateSetInfo());
  }
}
