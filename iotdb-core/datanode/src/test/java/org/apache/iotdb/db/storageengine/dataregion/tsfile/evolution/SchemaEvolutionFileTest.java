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

package org.apache.iotdb.db.storageengine.dataregion.tsfile.evolution;

import java.io.FileOutputStream;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class SchemaEvolutionFileTest {

  @After
  public void tearDown() throws Exception {
    clearSchemaEvolutionFile();
  }

  @Test
  public void testSchemaEvolutionFile() throws IOException {
    String filePath = TestConstant.BASE_OUTPUT_PATH + File.separator + "0.sevo";

    SchemaEvolutionFile schemaEvolutionFile = new SchemaEvolutionFile(filePath);

    // t1 -> t2, t2.s1 -> t2.s2, t3 -> t1
    List<SchemaEvolution> schemaEvolutionList =
        Arrays.asList(
            new TableRename("t1", "t2"),
            new ColumnRename("t2", "s1", "s2"),
            new TableRename("t3", "t1"));
    schemaEvolutionFile.append(schemaEvolutionList);

    EvolvedSchema evolvedSchema = schemaEvolutionFile.readAsSchema();
    assertEquals("t1", evolvedSchema.getOriginalTableName("t2"));
    assertEquals("s1", evolvedSchema.getOriginalColumnName("t2", "s2"));
    assertEquals("t3", evolvedSchema.getOriginalTableName("t1"));
    // not evolved, should remain the same
    assertEquals("t4", evolvedSchema.getOriginalTableName("t4"));
    assertEquals("s3", evolvedSchema.getOriginalColumnName("t2", "s3"));

    // t1 -> t2 -> t3, t2.s1 -> t2.s2 -> t3.s1, t3 -> t1 -> t2
    schemaEvolutionList =
        Arrays.asList(
            new TableRename("t2", "t3"),
            new ColumnRename("t3", "s2", "s1"),
            new TableRename("t1", "t2"));
    schemaEvolutionFile.append(schemaEvolutionList);
    evolvedSchema = schemaEvolutionFile.readAsSchema();
    assertEquals("t1", evolvedSchema.getOriginalTableName("t3"));
    assertEquals("s1", evolvedSchema.getOriginalColumnName("t3", "s1"));
    assertEquals("t3", evolvedSchema.getOriginalTableName("t2"));
    // not evolved, should remain the same
    assertEquals("t4", evolvedSchema.getOriginalTableName("t4"));
    assertEquals("s3", evolvedSchema.getOriginalColumnName("t2", "s3"));
  }

  private void clearSchemaEvolutionFile() {
    File dir = new File(TestConstant.BASE_OUTPUT_PATH);
    File[] files = dir.listFiles(f -> f.getName().endsWith(SchemaEvolutionFile.FILE_SUFFIX));
    if (files != null) {
      for (File file : files) {
        file.delete();
      }
    }
  }

  @Test
  public void testRecover() throws IOException {
    String filePath = TestConstant.BASE_OUTPUT_PATH + File.separator + "0.sevo";

    SchemaEvolutionFile schemaEvolutionFile = new SchemaEvolutionFile(filePath);
    List<SchemaEvolution> schemaEvolutionList =
        Arrays.asList(
            new TableRename("t1", "t2"),
            new ColumnRename("t2", "s1", "s2"),
            new TableRename("t3", "t1"));
    schemaEvolutionFile.append(schemaEvolutionList);

    File dir = new File(TestConstant.BASE_OUTPUT_PATH);
    File[] files = dir.listFiles(f -> f.getName().endsWith(SchemaEvolutionFile.FILE_SUFFIX));
    assertNotNull(files);
    assertEquals(1, files.length);
    assertEquals(24, SchemaEvolutionFile.parseValidLength(files[0].getName()));

    try (FileOutputStream fileOutputStream = new FileOutputStream(files[0], true)) {
      fileOutputStream.write(new byte[100]);
    }

    schemaEvolutionFile =  new SchemaEvolutionFile(files[0].getAbsolutePath());
    EvolvedSchema evolvedSchema = schemaEvolutionFile.readAsSchema();
    assertEquals("t1", evolvedSchema.getOriginalTableName("t2"));
    assertEquals("s1", evolvedSchema.getOriginalColumnName("t2", "s2"));
    assertEquals("t3", evolvedSchema.getOriginalTableName("t1"));
    // not evolved, should remain the same
    assertEquals("t4", evolvedSchema.getOriginalTableName("t4"));
    assertEquals("s3", evolvedSchema.getOriginalColumnName("t2", "s3"));
  }
}
