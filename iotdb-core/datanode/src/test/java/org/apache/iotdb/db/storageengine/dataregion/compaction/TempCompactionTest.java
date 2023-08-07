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

package org.apache.iotdb.db.storageengine.dataregion.compaction;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.RewriteCrossSpaceCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.CrossCompactionTaskResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

public class TempCompactionTest extends AbstractCompactionTest {

  @Before
  public void setup()
      throws IOException, InterruptedException, MetadataException, WriteProcessException {
    super.setUp();
  }

  private static final String FILES_TEXT =
      "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352361365-8-0-0.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352366895-9-0-0.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352373640-10-0-0.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352379191-11-0-0.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352384746-12-0-0.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352390254-13-0-0.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352395747-14-0-0.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352401194-15-0-0.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352406667-16-0-1.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352412149-17-0-0.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352775744-19-0-0.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352783673-20-0-0.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352791165-21-0-0.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352799362-22-0-0.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352807878-23-0-0.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352815455-24-0-0.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352821460-25-0-0.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352828927-26-0-0.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352833419-27-0-0.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352838178-28-0-0.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352844701-29-0-0.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352849287-30-0-0.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352855467-31-0-0.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352864219-33-0-0.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352874227-34-0-0.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352884929-37-0-0.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352889611-39-0-0.tsfile\n"
          + "/data/iotdb-data/datanode/data/sequence/root.TQ15A/19/0/1688352913163-44-0-0.tsfile\n"
          + "\n"
          + "/data/iotdb-data/datanode/data/unsequence/root.TQ15A/19/0/1688352944729-49-1-0.tsfile";

  @Test
  public void test0() throws IOException {
    List<TsFileResource> seq = getTsFileResources(new File("/Users/caozhijia/Desktop/2750"));
    tsFileManager.addAll(seq, false);
    List<TsFileResource> unSeq = getTsFileResources(new File("/Users/caozhijia/Desktop/unseq2750"));
    tsFileManager.addAll(unSeq, false);

    List<CrossCompactionTaskResource> crossCompactionTaskResources =
        new RewriteCrossSpaceCompactionSelector("root.test1.db_0", "4", 2750, tsFileManager)
            .selectCrossSpaceTask(seq, unSeq);

    //    InnerSpaceCompactionTask task =
    //        new InnerSpaceCompactionTask(
    //            0,
    //            tsFileManager,
    //            tsFileResources,
    //            false,
    //            new FastCompactionPerformer(false),
    //            new AtomicInteger(0),
    //            0);
  }

  private static List<TsFileResource> getTsFileResources(File dir) throws IOException {
    // get all seq files under the time partition dir
    List<File> tsFiles =
        Arrays.asList(
            Objects.requireNonNull(dir.listFiles(file -> file.getName().endsWith(TSFILE_SUFFIX))));
    // sort the seq files with timestamp
    tsFiles.sort(
        (f1, f2) -> {
          int timeDiff =
              Long.compareUnsigned(
                  Long.parseLong(f1.getName().split("-")[0]),
                  Long.parseLong(f2.getName().split("-")[0]));
          return timeDiff == 0
              ? Long.compareUnsigned(
                  Long.parseLong(f1.getName().split("-")[1]),
                  Long.parseLong(f2.getName().split("-")[1]))
              : timeDiff;
        });
    List<TsFileResource> tsFileResources = new ArrayList<>();
    for (File tsFile : tsFiles) {
      TsFileResource resource = new TsFileResource(tsFile);
      resource.deserialize();
      resource.close();
      tsFileResources.add(resource);
    }
    return tsFileResources;
  }

  @Test
  public void test1() throws IOException {
    String seqFilePath = "/Users/shuww/compactionTest/seq/",
        unseqFileDirPath = "/Users/shuww/compactionTest/unseq/";
    List<TsFileResource> seqTsFileResources = loadAndSortFile(seqFilePath);
    tsFileManager.addAll(seqTsFileResources, true);
    List<TsFileResource> unseqTsFileResources = loadAndSortFile(unseqFileDirPath);
    tsFileManager.addAll(unseqTsFileResources, false);

    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqTsFileResources,
            unseqTsFileResources,
            new FastCompactionPerformer(true),
            new AtomicInteger(0),
            0,
            0);
    Assert.assertTrue(task.start());
  }

  private List<TsFileResource> loadAndSortFile(String dirPath) throws IOException {
    File dir = new File(dirPath);
    // get all seq files under the time partition dir
    List<File> tsFiles =
        Arrays.asList(
            Objects.requireNonNull(
                dir.listFiles(
                    file ->
                        file.getName().endsWith(TSFILE_SUFFIX)
                            && FILES_TEXT.contains(file.getName()))));
    // sort the seq files with timestamp
    tsFiles.sort(
        (f1, f2) -> {
          int timeDiff =
              Long.compareUnsigned(
                  Long.parseLong(f1.getName().split("-")[0]),
                  Long.parseLong(f2.getName().split("-")[0]));
          return timeDiff == 0
              ? Long.compareUnsigned(
                  Long.parseLong(f1.getName().split("-")[1]),
                  Long.parseLong(f2.getName().split("-")[1]))
              : timeDiff;
        });
    List<TsFileResource> tsFileResources = new ArrayList<>();
    for (File tsFile : tsFiles) {
      TsFileResource resource = new TsFileResource(tsFile);
      resource.deserialize();
      resource.close();
      tsFileResources.add(resource);
    }
    return tsFileResources;
  }
}
