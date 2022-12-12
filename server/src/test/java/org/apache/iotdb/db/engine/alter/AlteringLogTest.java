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
package org.apache.iotdb.db.engine.alter;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.alter.log.AlteringLogAnalyzer;
import org.apache.iotdb.db.engine.alter.log.AlteringLogger;
import org.apache.iotdb.db.engine.compaction.log.TsFileIdentifier;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class AlteringLogTest {

  String path = AlteringLogger.ALTERING_LOG_NAME;

  String f1 =
      "data"
          .concat(File.separator)
          .concat("data")
          .concat(File.separator)
          .concat("sequence")
          .concat(File.separator)
          .concat("root.alt1")
          .concat(File.separator)
          .concat("0")
          .concat(File.separator)
          .concat("0")
          .concat(File.separator)
          .concat("1-1-0-0.tsfile");

  String f2 =
      "data"
          .concat(File.separator)
          .concat("data")
          .concat(File.separator)
          .concat("sequence")
          .concat(File.separator)
          .concat("root.alt1")
          .concat(File.separator)
          .concat("0")
          .concat(File.separator)
          .concat("0")
          .concat(File.separator)
          .concat("2-2-0-0.tsfile");

  @Before
  public void setUp() throws Exception {
    try {
      File f = FSFactoryProducer.getFSFactory().getFile(path);
      if (f.exists() && !f.delete()) {
        throw new RuntimeException("can not delete " + f.getAbsolutePath());
      }
      f.createNewFile();
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void alterLogTest() {

    File f = FSFactoryProducer.getFSFactory().getFile(path);
    try (AlteringLogger logger = new AlteringLogger(f)) {
      PartialPath pm1 = new PartialPath("root.alt1.d1", "m1");
      PartialPath pm2 = new PartialPath("root.alt1.d2", "m1");
      logger.addAlterParam(pm1, TSEncoding.TS_2DIFF, CompressionType.GZIP);
      logger.addAlterParam(pm2, TSEncoding.GORILLA, CompressionType.SNAPPY);
      logger.clearBegin();
      logger.doneFile(new TsFileResource(new File(f1)));
      logger.doneFile(new TsFileResource(new File(f2)));
      logger.close();

      AlteringLogAnalyzer analyzer = new AlteringLogAnalyzer(f);
      analyzer.analyzer();

      List<Pair<String, Pair<TSEncoding, CompressionType>>> alterList = analyzer.getAlterList();
      Assert.assertNotNull(alterList);
      Assert.assertEquals(alterList.size(), 2);
      Pair<String, Pair<TSEncoding, CompressionType>> p1 = alterList.get(0);
      Pair<String, Pair<TSEncoding, CompressionType>> p2 = alterList.get(1);
      Assert.assertEquals(p1.left, pm1.getFullPath());
      Assert.assertEquals(p2.left, pm2.getFullPath());
      Assert.assertEquals(p1.right.left, TSEncoding.TS_2DIFF);
      Assert.assertEquals(p1.right.right, CompressionType.GZIP);
      Assert.assertEquals(p2.right.left, TSEncoding.GORILLA);
      Assert.assertEquals(p2.right.right, CompressionType.SNAPPY);
      Assert.assertTrue(analyzer.isClearBegin());
      Set<TsFileIdentifier> doneFiles = analyzer.getDoneFiles();
      Assert.assertNotNull(alterList);
      Assert.assertEquals(alterList.size(), 2);
      Iterator<TsFileIdentifier> it = doneFiles.iterator();
      TsFileIdentifier if1 = it.next();
      TsFileIdentifier if2 = it.next();
      Assert.assertEquals(TsFileIdentifier.getFileIdentifierFromFilePath(f1), if1);
      Assert.assertEquals(TsFileIdentifier.getFileIdentifierFromFilePath(f2), if2);

    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @After
  public void tearDown() {
    try {
      FileUtils.forceDelete(new File(path));
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }
}
