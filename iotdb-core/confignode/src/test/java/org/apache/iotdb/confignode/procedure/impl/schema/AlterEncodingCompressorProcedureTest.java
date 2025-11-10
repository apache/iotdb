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
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class AlterEncodingCompressorProcedureTest {
  @Test
  public void serializeDeserializeTest() throws IllegalPathException, IOException {
    final String queryId = "1";
    final PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.sg1.**"));
    patternTree.appendPathPattern(new PartialPath("root.sg2.*.s1"));
    patternTree.constructTree();
    final AlterEncodingCompressorProcedure alterEncodingCompressorProcedure =
        new AlterEncodingCompressorProcedure(
            false, queryId, patternTree, false, (byte) 0, (byte) 0, false);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    alterEncodingCompressorProcedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.ALTER_ENCODING_COMPRESSOR_PROCEDURE.getTypeCode(), byteBuffer.getShort());

    final AlterEncodingCompressorProcedure deserializedProcedure =
        new AlterEncodingCompressorProcedure(false);
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(alterEncodingCompressorProcedure, deserializedProcedure);
  }
}
