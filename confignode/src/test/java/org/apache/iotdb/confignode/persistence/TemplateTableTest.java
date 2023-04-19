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
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.confignode.persistence.schema.TemplateTable;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
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
  public void testSnapshot() throws IOException, MetadataException {
    int n = 2;
    String templateName = "template_test";

    List<Template> templates = new ArrayList<>();
    // create schema template
    for (int i = 0; i < n; i++) {
      String templateNameTmp = templateName + "_" + i;
      Template template =
          i == 1 ? newAlignedSchemaTemplate(templateNameTmp) : newSchemaTemplate(templateNameTmp);
      templates.add(template);
      templateTable.createTemplate(template);
    }

    templateTable.processTakeSnapshot(snapshotDir);
    templateTable.clear();
    templateTable.processLoadSnapshot(snapshotDir);

    // show nodes in schema template
    for (int i = 0; i < n; i++) {
      String templateNameTmp = templateName + "_" + i;
      Template template = templates.get(i);
      Assert.assertEquals(template, templateTable.getTemplate(templateNameTmp));
    }
  }

  private Template newSchemaTemplate(String name) throws IllegalPathException {
    List<String> measurements = Arrays.asList(name + "_" + "temperature", name + "_" + "status");
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.FLOAT, TSDataType.BOOLEAN);
    List<TSEncoding> encodings = Arrays.asList(TSEncoding.RLE, TSEncoding.PLAIN);
    List<CompressionType> compressors =
        Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY);
    return new Template(name, measurements, dataTypes, encodings, compressors);
  }

  private Template newAlignedSchemaTemplate(String name) throws IllegalPathException {
    List<String> measurements = Arrays.asList(name + "_" + "lat", name + "_" + "lon");
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT);
    List<TSEncoding> encodings = Arrays.asList(TSEncoding.GORILLA, TSEncoding.GORILLA);
    List<CompressionType> compressors =
        Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY);
    return new Template(name, measurements, dataTypes, encodings, compressors, true);
  }
}
