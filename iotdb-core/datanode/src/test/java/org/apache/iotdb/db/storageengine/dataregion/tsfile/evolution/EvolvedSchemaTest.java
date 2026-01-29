/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.tsfile.evolution;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class EvolvedSchemaTest {

  @Test
  public void testMerge() {
    // t1 -> t2, t2.s1 -> t2.s2, t3 -> t1
    List<SchemaEvolution> schemaEvolutionList =
        Arrays.asList(
            new TableRename("t1", "t2"),
            new ColumnRename("t2", "s1", "s2", TSDataType.INT32),
            new TableRename("t3", "t1"));
    EvolvedSchema oldSchema = new EvolvedSchema();
    EvolvedSchema allSchema = new EvolvedSchema();
    schemaEvolutionList.forEach(schemaEvolution -> schemaEvolution.applyTo(oldSchema));
    schemaEvolutionList.forEach(schemaEvolution -> schemaEvolution.applyTo(allSchema));

    // t1 -> t2 -> t3, t2.s1 -> t2.s2 -> t3.s1, t3 -> t1 -> t2
    schemaEvolutionList =
        Arrays.asList(
            new TableRename("t2", "t3"),
            new ColumnRename("t3", "s2", "s1", TSDataType.INT32),
            new TableRename("t1", "t2"));
    EvolvedSchema newSchema = new EvolvedSchema();
    schemaEvolutionList.forEach(schemaEvolution -> schemaEvolution.applyTo(newSchema));
    schemaEvolutionList.forEach(schemaEvolution -> schemaEvolution.applyTo(allSchema));

    EvolvedSchema mergedShema = EvolvedSchema.merge(oldSchema, newSchema);

    assertEquals(allSchema, mergedShema);
  }

  @Test
  public void testCovert() {
    // t1 -> t2, t2.s1 -> t2.s2, t3 -> t1
    List<SchemaEvolution> schemaEvolutionList =
        Arrays.asList(
            new TableRename("t1", "t2"),
            new ColumnRename("t2", "s1", "s2", TSDataType.INT32),
            new TableRename("t3", "t1"));
    EvolvedSchema oldSchema = new EvolvedSchema();
    schemaEvolutionList.forEach(schemaEvolution -> schemaEvolution.applyTo(oldSchema));

    List<SchemaEvolution> convertedSchemaEvolutions = oldSchema.toSchemaEvolutions();
    EvolvedSchema newSchema = new EvolvedSchema();
    convertedSchemaEvolutions.forEach(schemaEvolution -> schemaEvolution.applyTo(newSchema));

    assertEquals(oldSchema, newSchema);
  }

  @Test
  public void testTableRename() {
    EvolvedSchema schema = new EvolvedSchema();
    // t1 -> t2
    SchemaEvolution schemaEvolution = new TableRename("t1", "t2");
    schemaEvolution.applyTo(schema);
    assertEquals("t1", schema.getOriginalTableName("t2"));
    assertEquals("", schema.getOriginalTableName("t1"));
    assertEquals("t2", schema.getFinalTableName("t1"));
    assertEquals("t2", schema.getFinalTableName("t2"));
    // t1 -> t2 -> t3
    schemaEvolution = new TableRename("t2", "t3");
    schemaEvolution.applyTo(schema);
    assertEquals("t1", schema.getOriginalTableName("t3"));
    assertEquals("", schema.getOriginalTableName("t2"));
    assertEquals("t3", schema.getFinalTableName("t1"));
    assertEquals("t2", schema.getFinalTableName("t2"));
    // t1 -> t2 -> t3 -> t1
    schemaEvolution = new TableRename("t3", "t1");
    schemaEvolution.applyTo(schema);
    assertEquals("t1", schema.getOriginalTableName("t1"));
    assertEquals("", schema.getOriginalTableName("t3"));
    assertEquals("t1", schema.getFinalTableName("t1"));
    assertEquals("t3", schema.getFinalTableName("t3"));
  }

  @Test
  public void testColumnRename() {
    EvolvedSchema schema = new EvolvedSchema();
    // s1 -> s2
    SchemaEvolution schemaEvolution = new ColumnRename("t1", "s1", "s2");
    schemaEvolution.applyTo(schema);
    assertEquals("s1", schema.getOriginalColumnName("t1", "s2"));
    assertEquals("", schema.getOriginalColumnName("t1", "s1"));
    assertEquals("s2", schema.getFinalColumnName("t1", "s1"));
    assertEquals("s2", schema.getFinalColumnName("t1", "s2"));
    // s1 -> s2 -> s3
    schemaEvolution = new ColumnRename("t1", "s2", "s3");
    schemaEvolution.applyTo(schema);
    assertEquals("s1", schema.getOriginalColumnName("t1", "s3"));
    assertEquals("", schema.getOriginalColumnName("t1", "s2"));
    assertEquals("s3", schema.getFinalColumnName("t1", "s1"));
    assertEquals("s2", schema.getFinalColumnName("t1", "s2"));
    // s1 -> s2 -> s3 -> s1
    schemaEvolution = new ColumnRename("t1", "s3", "s1");
    schemaEvolution.applyTo(schema);
    assertEquals("s1", schema.getOriginalColumnName("t1", "s1"));
    assertEquals("", schema.getOriginalColumnName("t1", "s3"));
    assertEquals("s1", schema.getFinalColumnName("t1", "s1"));
    assertEquals("s3", schema.getFinalColumnName("t3", "s3"));
  }
}
