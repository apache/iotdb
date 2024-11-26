/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.tools;

import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.tsfile.read.query.executor.TableQueryExecutor;
import org.apache.tsfile.read.reader.block.TsBlockReader;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TsfiletoolsTest {
  private final String testDir = "target" + File.separator + "csvTest";
  private final String csvFile = testDir + File.separator + "data.csv";

  private final String wrongCsvFile = testDir + File.separator + "dataWrong.csv";
  private final String schemaFile = testDir + File.separator + "schemaFile.txt";

  private final String failedDir = testDir + File.separator + "failed";

  float[] tmpResult2 = new float[20];
  float[] tmpResult3 = new float[20];
  float[] tmpResult5 = new float[20];

  @Before
  public void setUp() {
    new File(testDir).mkdirs();
    genCsvFile(20);
    genWrongCsvFile(100);
    genSchemaFile();
  }

  public void genSchemaFile() {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(schemaFile))) {
      writer.write("table_name=root.db1");
      writer.newLine();
      writer.write("time_precision=ms");
      writer.newLine();
      writer.write("has_header=true");
      writer.newLine();
      writer.write("separator=,");
      writer.newLine();
      writer.write("null_format=\\N");
      writer.newLine();
      writer.newLine();
      writer.write("id_columns");
      writer.newLine();
      writer.write("tmp1");
      writer.newLine();
      writer.write("time_column=time");
      writer.newLine();
      writer.write("csv_columns");
      writer.newLine();
      writer.write("time INT64,");
      writer.newLine();
      writer.write("tmp1 TEXT,");
      writer.newLine();
      writer.write("tmp2 FLOAT,");
      writer.newLine();
      writer.write("tmp3 FLOAT,");
      writer.newLine();
      writer.write("SKIP,");
      writer.newLine();
      writer.write("tmp5 FLOAT");
    } catch (IOException e) {
      throw new RuntimeException("Failed to generate schema file", e);
    }
  }

  public void genWrongCsvFile(int rows) {

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(wrongCsvFile))) {
      writer.write("time,tmp1,tmp2,tmp3,tmp4,tmp5");
      writer.newLine();
      Random random = new Random();
      long timestamp = System.currentTimeMillis();

      for (int i = 0; i < rows; i++) {
        timestamp = timestamp + i;
        String tmp1 = "s1";
        float tmp2 = random.nextFloat();
        float tmp3 = random.nextFloat();
        float tmp4 = random.nextFloat();
        float tmp5 = random.nextFloat();
        if (i % 99 == 0) {
          writer.write(
              timestamp + "aa" + "," + tmp1 + "," + tmp2 + "," + tmp3 + "," + tmp4 + "," + tmp5);
        } else {
          writer.write(timestamp + "," + tmp1 + "," + tmp2 + "," + tmp3 + "," + tmp4 + "," + tmp5);
        }

        writer.newLine();
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to generate wrong CSV file", e);
    }
  }

  public void genCsvFile(int rows) {

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFile))) {
      writer.write("time,tmp1,tmp2,tmp3,tmp4,tmp5");
      writer.newLine();
      Random random = new Random();
      long timestamp = System.currentTimeMillis();

      for (int i = 0; i < rows; i++) {
        timestamp = timestamp + i;
        String tmp1 = "s1";
        float tmp2 = random.nextFloat();
        float tmp3 = random.nextFloat();
        float tmp4 = random.nextFloat();
        float tmp5 = random.nextFloat();
        tmpResult2[i] = tmp2;
        tmpResult3[i] = tmp3;
        tmpResult5[i] = tmp5;
        writer.write(timestamp + "," + tmp1 + "," + tmp2 + "," + tmp3 + "," + tmp4 + "," + tmp5);
        writer.newLine();
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to generate CSV file", e);
    }
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(new File(testDir));
  }

  @Test
  public void testCsvToTsfile() throws Exception {
    String scFilePath = new File(schemaFile).getAbsolutePath();
    String csvFilePath = new File(csvFile).getAbsolutePath();
    String targetPath = new File(testDir).getAbsolutePath();
    String dataTsfilePath = new File(targetPath + File.separator + "data.tsfile").getAbsolutePath();
    String[] args = new String[] {"-s" + csvFilePath, "-schema" + scFilePath, "-t" + targetPath};
    TsFileTool.main(args);
    List<String> columns = new ArrayList<>();
    columns.add("tmp2");
    columns.add("tmp3");
    columns.add("tmp5");
    try (TsFileSequenceReader sequenceReader = new TsFileSequenceReader(dataTsfilePath)) {
      TableQueryExecutor tableQueryExecutor =
          new TableQueryExecutor(
              new MetadataQuerierByFileImpl(sequenceReader),
              new CachedChunkLoaderImpl(sequenceReader),
              TableQueryExecutor.TableQueryOrdering.DEVICE);
      final TsBlockReader reader = tableQueryExecutor.query("root.db1", columns, null, null, null);
      assertTrue(reader.hasNext());
      int cnt = 0;
      while (reader.hasNext()) {
        final TsBlock result = reader.next();
        float[] floats_tmp2 = result.getColumn(0).getFloats();
        float[] floats_tmp3 = result.getColumn(1).getFloats();
        float[] floats_tmp5 = result.getColumn(2).getFloats();
        for (int i = 0; i < 20; i++) {
          assertEquals(tmpResult2[i], floats_tmp2[i], 0);
          assertEquals(tmpResult3[i], floats_tmp3[i], 0);
          assertEquals(tmpResult5[i], floats_tmp5[i], 0);
        }
        cnt += result.getPositionCount();
      }
      assertEquals(20, cnt);
    }
  }

  @Test
  public void testCsvToTsfileFailed() {
    String scFilePath = new File(schemaFile).getAbsolutePath();
    String csvFilePath = new File(wrongCsvFile).getAbsolutePath();
    String targetPath = new File(testDir).getAbsolutePath();
    String fd = new File(failedDir).getAbsolutePath();
    String[] args =
        new String[] {
          "-s" + csvFilePath, "-schema" + scFilePath, "-t" + targetPath, "-fail_dir" + fd
        };
    TsFileTool.main(args);
    assertTrue(new File(failedDir + File.separator + "dataWrong.csv").exists());
    try (BufferedReader br =
        new BufferedReader(new FileReader(failedDir + File.separator + "dataWrong.csv"))) {
      int num = 0;
      while (br.readLine() != null) {
        num++;
      }
      assertEquals(101, num);
    } catch (IOException e) {
      throw new RuntimeException("IOException occurred while reading file", e);
    }
  }
}
