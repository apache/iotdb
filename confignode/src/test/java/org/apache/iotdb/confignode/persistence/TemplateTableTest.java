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

package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.confignode.consensus.request.write.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.persistence.schema.TemplateTable;
import org.apache.iotdb.confignode.rpc.thrift.TGetTemplateResp;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.CreateSchemaTemplateStatement;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.constant.TestConstant.BASE_OUTPUT_PATH;

public class TemplateTableTest {

  private static TemplateTable templateTable;
  private static final File snapshotDir = new File(BASE_OUTPUT_PATH, "snapshot");

  @BeforeClass
  public static void setup() throws IOException {
    templateTable = new TemplateTable();
    if (!snapshotDir.exists()) {
      snapshotDir.mkdirs();
    }
  }

  @AfterClass
  public static void cleanup() throws IOException {
    templateTable.clear();
    if (snapshotDir.exists()) {
      FileUtils.deleteDirectory(snapshotDir);
    }
  }

  @Test
  public void testSnapshot()
      throws IOException, TException, IllegalPathException, ClassNotFoundException {
    int n = 2;
    String templateName = "template_test";

    List<Template> templates = new ArrayList<>();
    // create schema template
    for (int i = 0; i < n; i++) {
      String templateNameTmp = templateName + "_" + i;
      CreateSchemaTemplateStatement statement = null;
      if (i == 1) {
        statement = newCreateSchemaTemplateStatementAlign(templateNameTmp);
      } else {
        statement = newCreateSchemaTemplateStatement(templateNameTmp);
      }
      Template template = new Template(statement);
      templates.add(template);
      CreateSchemaTemplatePlan createSchemaTemplatePlan =
          new CreateSchemaTemplatePlan(Template.template2ByteBuffer(template).array());
      templateTable.createTemplate(createSchemaTemplatePlan);
    }

    templateTable.processTakeSnapshot(snapshotDir);
    templateTable.clear();
    templateTable.processLoadSnapshot(snapshotDir);

    // show nodes in schema template
    for (int i = 0; i < n; i++) {
      String templateNameTmp = templateName + "_" + i;
      TGetTemplateResp templateResp = templateTable.getMatchedTemplateByName(templateNameTmp);
      Template template = templates.get(i);
      Template serTemplate =
          Template.byteBuffer2Template(ByteBuffer.wrap(templateResp.getTemplate()));
      Assert.assertEquals(template, serTemplate);
    }
  }

  private CreateSchemaTemplateStatement newCreateSchemaTemplateStatement(String name) {
    List<List<String>> measurements =
        Arrays.asList(
            Arrays.asList(name + "_" + "temperature"), Arrays.asList(name + "_" + "status"));
    List<List<TSDataType>> dataTypes =
        Arrays.asList(Arrays.asList(TSDataType.FLOAT), Arrays.asList(TSDataType.BOOLEAN));
    List<List<TSEncoding>> encodings =
        Arrays.asList(Arrays.asList(TSEncoding.RLE), Arrays.asList(TSEncoding.PLAIN));
    List<List<CompressionType>> compressors =
        Arrays.asList(Arrays.asList(CompressionType.SNAPPY), Arrays.asList(CompressionType.SNAPPY));
    CreateSchemaTemplateStatement createSchemaTemplateStatement =
        new CreateSchemaTemplateStatement(name, measurements, dataTypes, encodings, compressors);
    return createSchemaTemplateStatement;
  }

  private CreateSchemaTemplateStatement newCreateSchemaTemplateStatementAlign(String name) {
    List<List<String>> measurements =
        Arrays.asList(Arrays.asList(name + "_" + "lat", name + "_" + "lon"));
    List<List<TSDataType>> dataTypes =
        Arrays.asList(Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT));
    List<List<TSEncoding>> encodings =
        Arrays.asList(Arrays.asList(TSEncoding.GORILLA, TSEncoding.GORILLA));
    List<List<CompressionType>> compressors =
        Arrays.asList(Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY));
    CreateSchemaTemplateStatement createSchemaTemplateStatement =
        new CreateSchemaTemplateStatement(name, measurements, dataTypes, encodings, compressors);
    return createSchemaTemplateStatement;
  }
}
