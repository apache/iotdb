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

package org.apache.iotdb.db.pipe.resource;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.fail;

public class PipeTsFileResourceManagerTest {

  public static final String TMP_DIR =
      "target"
          + File.separator
          + "PipeTsFileHolderTest"
          + File.separator
          + IoTDBConstant.SEQUENCE_FLODER_NAME;
  private final String tsFileName = TMP_DIR + File.separator + "test.tsfile";
  private final String modsFileName = tsFileName + ".mods";

  public final List<String> fileNameList = new LinkedList<>();

  public final String DEFAULT_TEMPLATE = "template";

  private PipeTsFileResourceManager pipeTsFileResourceManager;

  @Before
  public void setUp() throws Exception {
    createTsfile(tsFileName);
    fileNameList.add(tsFileName);
    creatModsFile(modsFileName);
    fileNameList.add(modsFileName);

    pipeTsFileResourceManager = new PipeTsFileResourceManager();
  }

  @After
  public void tearDown() throws Exception {
    File pipeFolder = new File("target" + File.separator + "PipeTsFileHolderTest");
    if (pipeFolder.exists()) {
      FileUtils.deleteDirectory(pipeFolder);
    }
  }

  @Test
  public void testIncreaseTsfile() throws IOException {
    File originTsfile = new File(tsFileName);
    File originModFile = new File(modsFileName);
    Assert.assertEquals(0, pipeTsFileResourceManager.getFileReferenceCount(originTsfile));
    Assert.assertEquals(0, pipeTsFileResourceManager.getFileReferenceCount(originModFile));

    File pipeTsfile = pipeTsFileResourceManager.increaseFileReference(originTsfile, true);
    File pipeModFile = pipeTsFileResourceManager.increaseFileReference(originModFile, false);
    Assert.assertEquals(1, pipeTsFileResourceManager.getFileReferenceCount(pipeTsfile));
    Assert.assertEquals(1, pipeTsFileResourceManager.getFileReferenceCount(pipeModFile));

    // test use hardlinkTsFile to increase reference counts
    pipeTsFileResourceManager.increaseFileReference(pipeTsfile, true);
    Assert.assertEquals(2, pipeTsFileResourceManager.getFileReferenceCount(pipeTsfile));

    // test use copyFile to increase reference counts
    pipeTsFileResourceManager.increaseFileReference(pipeModFile, false);
    Assert.assertEquals(2, pipeTsFileResourceManager.getFileReferenceCount(pipeModFile));
  }

  @Test
  public void testDecreaseTsfile() throws IOException {
    File originFile = new File(tsFileName);
    File originModFile = new File(modsFileName);

    pipeTsFileResourceManager.decreaseFileReference(originFile);
    pipeTsFileResourceManager.decreaseFileReference(originModFile);
    Assert.assertEquals(0, pipeTsFileResourceManager.getFileReferenceCount(originFile));
    Assert.assertEquals(0, pipeTsFileResourceManager.getFileReferenceCount(originModFile));

    File pipeTsfile = pipeTsFileResourceManager.increaseFileReference(originFile, true);
    File pipeModFile = pipeTsFileResourceManager.increaseFileReference(originModFile, false);
    Assert.assertEquals(1, pipeTsFileResourceManager.getFileReferenceCount(pipeTsfile));
    Assert.assertEquals(1, pipeTsFileResourceManager.getFileReferenceCount(pipeModFile));
    Assert.assertTrue(Files.exists(pipeTsfile.toPath()));
    Assert.assertTrue(Files.exists(pipeModFile.toPath()));

    pipeTsFileResourceManager.decreaseFileReference(pipeTsfile);
    pipeTsFileResourceManager.decreaseFileReference(pipeModFile);
    Assert.assertEquals(0, pipeTsFileResourceManager.getFileReferenceCount(pipeTsfile));
    Assert.assertEquals(0, pipeTsFileResourceManager.getFileReferenceCount(pipeModFile));
    Assert.assertFalse(Files.exists(pipeTsfile.toPath()));
    Assert.assertFalse(Files.exists(pipeModFile.toPath()));
  }

  private void createTsfile(String tsfilePath) throws Exception {
    File file = new File(tsfilePath);
    if (file.exists()) {
      boolean ignored = file.delete();
    }

    Schema schema = new Schema();
    schema.extendTemplate(
        DEFAULT_TEMPLATE, new MeasurementSchema("sensor1", TSDataType.FLOAT, TSEncoding.RLE));
    schema.extendTemplate(
        DEFAULT_TEMPLATE, new MeasurementSchema("sensor2", TSDataType.INT32, TSEncoding.TS_2DIFF));
    schema.extendTemplate(
        DEFAULT_TEMPLATE, new MeasurementSchema("sensor3", TSDataType.INT32, TSEncoding.TS_2DIFF));

    TsFileWriter tsFileWriter = new TsFileWriter(file, schema);

    // construct TSRecord
    TSRecord tsRecord = new TSRecord(1617206403001L, "root.lemming.device1");
    DataPoint dPoint1 = new FloatDataPoint("sensor1", 1.1f);
    DataPoint dPoint2 = new IntDataPoint("sensor2", 12);
    DataPoint dPoint3 = new IntDataPoint("sensor3", 13);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);
    tsFileWriter.flushAllChunkGroups(); // flush above data to disk at once

    tsRecord = new TSRecord(1617206403002L, "root.lemming.device2");
    dPoint2 = new IntDataPoint("sensor2", 22);
    tsRecord.addTuple(dPoint2);
    tsFileWriter.write(tsRecord);
    tsFileWriter.flushAllChunkGroups(); // flush above data to disk at once

    tsRecord = new TSRecord(1617206403003L, "root.lemming.device3");
    dPoint1 = new FloatDataPoint("sensor1", 3.1f);
    dPoint2 = new IntDataPoint("sensor2", 32);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsFileWriter.write(tsRecord);
    tsFileWriter.flushAllChunkGroups(); // flush above data to disk at once

    tsRecord = new TSRecord(1617206403004L, "root.lemming.device1");
    dPoint1 = new FloatDataPoint("sensor1", 4.1f);
    dPoint2 = new IntDataPoint("sensor2", 42);
    dPoint3 = new IntDataPoint("sensor3", 43);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);
    tsFileWriter.flushAllChunkGroups(); // flush above data to disk at once

    // close TsFile
    tsFileWriter.close();
  }

  private void creatModsFile(String modsFilePath) throws IllegalPathException {
    Modification[] modifications =
        new Modification[] {
          new Deletion(new PartialPath("root.lemming.device1.sensor1"), 2, 1),
          new Deletion(new PartialPath("root.lemming.device1.sensor1"), 3, 2, 5),
          new Deletion(new PartialPath("root.lemming.**"), 11, 1, Long.MAX_VALUE)
        };

    try (ModificationFile mFile = new ModificationFile(modsFilePath)) {
      for (Modification mod : modifications) {
        mFile.write(mod);
      }
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }
}
