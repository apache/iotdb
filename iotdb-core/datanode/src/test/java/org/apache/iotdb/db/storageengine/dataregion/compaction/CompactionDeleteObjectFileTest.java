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

package org.apache.iotdb.db.storageengine.dataregion.compaction;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.SettleCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CompactionDeleteObjectFileTest extends AbstractCompactionTest {
  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
  }

  @Test
  public void test1() throws IOException {
    createTable("tsfile_table", 100);
    File dir = new File("/Users/shuww/Downloads/0708/1_副本");
    List<TsFileResource> resources = new ArrayList<>();
    for (File file : dir.listFiles()) {
      if (!file.getName().endsWith(".tsfile")) {
        continue;
      }
      TsFileResource resource = new TsFileResource(file);

      try (ModificationFile modificationFile = resource.getExclusiveModFile()) {
        modificationFile.write(
            new TableDeletionEntry(
                new DeletionPredicate(
                    "tsfile_table",
                    new IDPredicate.FullExactMatch(
                        new StringArrayDeviceID(new String[] {"tsfile_table", "1", "5", "3"})),
                    Arrays.asList("file")),
                new TimeRange(-1, 0)));
        modificationFile.write(
            new TableDeletionEntry(
                new DeletionPredicate(
                    "tsfile_table",
                    new IDPredicate.FullExactMatch(
                        new StringArrayDeviceID(new String[] {"tsfile_table", "1", "5", "3"})),
                    Arrays.asList("file")),
                new TimeRange(2, 2)));
      }
      resource.deserialize();
      resources.add(resource);
    }

    //        InnerSpaceCompactionTask task =
    //            new InnerSpaceCompactionTask(
    //                0, tsFileManager, resources, true, new ReadChunkCompactionPerformer(), 0);
    SettleCompactionTask task =
        new SettleCompactionTask(
            0,
            tsFileManager,
            resources,
            Collections.emptyList(),
            true,
            new FastCompactionPerformer(false),
            0);
    task.start();
  }

  public void createTable(String tableName, long ttl) {
    TsTable tsTable = new TsTable(tableName);
    tsTable.addColumnSchema(new TagColumnSchema("id_column", TSDataType.STRING));
    tsTable.addColumnSchema(
        new FieldColumnSchema("s1", TSDataType.STRING, TSEncoding.PLAIN, CompressionType.LZ4));
    tsTable.addProp(TsTable.TTL_PROPERTY, ttl + "");
    DataNodeTableCache.getInstance().preUpdateTable("Downloads", tsTable, null);
    DataNodeTableCache.getInstance().commitUpdateTable("Downloads", tableName, null);
  }
}
