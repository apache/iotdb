/**
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
package org.apache.iotdb.db.engine.overflow.io;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.version.SysTimeVersionController;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OverflowResourceTest {

  private OverflowResource work;
  private File insertFile;
  private String insertFileName = "unseqTsFile";
  private String folderPath = "overflow";
  private String dataPath = "1";
  private OverflowMemtable memtable = new OverflowMemtable();

  @Before
  public void setUp() throws Exception {
    work = new OverflowResource(folderPath, dataPath, SysTimeVersionController.INSTANCE,"processorName");
    insertFile = new File(new File(folderPath, dataPath), insertFileName);
  }

  @After
  public void tearDown() throws Exception {
    work.close();
    memtable.clear();
    EnvironmentUtils.cleanDir(folderPath);
  }

  private void removeFlushedMemTable(IMemTable memTable, TsFileIOWriter overflowIOWriter) {
//    overflowIOWriter.mergeChunkGroupMetaData();
  }

  @Test
  public void testOverflowInsert() throws IOException {
    OverflowTestUtils.produceInsertData(memtable);
    QueryContext context = new QueryContext();
    work.flush(OverflowTestUtils.getFileSchema(), memtable.getMemTabale(),
        "processorName", 0, this::removeFlushedMemTable);
    List<ChunkMetaData> chunkMetaDatas = work.getInsertMetadatas(OverflowTestUtils.deviceId1,
        OverflowTestUtils.measurementId1, OverflowTestUtils.dataType2, context);
    assertEquals(0, chunkMetaDatas.size());
    work.appendMetadatas();
    chunkMetaDatas = work
        .getInsertMetadatas(OverflowTestUtils.deviceId1, OverflowTestUtils.measurementId1,
            OverflowTestUtils.dataType1, context);
    assertEquals(1, chunkMetaDatas.size());
    ChunkMetaData chunkMetaData = chunkMetaDatas.get(0);
    assertEquals(OverflowTestUtils.dataType1, chunkMetaData.getTsDataType());
    assertEquals(OverflowTestUtils.measurementId1, chunkMetaData.getMeasurementUid());
    // close
    work.close();
    // append file
    long originlength = insertFile.length();
    FileOutputStream fileOutputStream = new FileOutputStream(insertFile, true);
    fileOutputStream.write(new byte[20]);
    fileOutputStream.close();
    assertEquals(originlength + 20, insertFile.length());
    work = new OverflowResource(folderPath, dataPath, SysTimeVersionController.INSTANCE, "processorName");
    chunkMetaDatas = work
        .getInsertMetadatas(OverflowTestUtils.deviceId1, OverflowTestUtils.measurementId1,
            OverflowTestUtils.dataType1, context);
    assertEquals(1, chunkMetaDatas.size());
    chunkMetaData = chunkMetaDatas.get(0);
    assertEquals(OverflowTestUtils.dataType1, chunkMetaData.getTsDataType());
    assertEquals(OverflowTestUtils.measurementId1, chunkMetaData.getMeasurementUid());
    assertEquals(originlength, insertFile.length());
  }
}
