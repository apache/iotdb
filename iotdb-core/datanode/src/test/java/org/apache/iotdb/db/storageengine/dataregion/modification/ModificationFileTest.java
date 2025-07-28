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

package org.apache.iotdb.db.storageengine.dataregion.modification;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.recover.CompactionRecoverManager;
import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate.FullExactMatch;
import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate.NOP;
import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate.SegmentExactMatch;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.read.common.TimeRange;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ModificationFileTest {

  @Test
  public void readMyWrite() {
    String tempFileName = TestConstant.BASE_OUTPUT_PATH.concat("mod.temp");
    ModEntry[] modifications =
        new ModEntry[] {
          new TreeDeletionEntry(new MeasurementPath(new String[] {"d1", "s1"}), 1),
          new TreeDeletionEntry(new MeasurementPath(new String[] {"d1", "s2"}), 2),
          new TreeDeletionEntry(new MeasurementPath(new String[] {"d1", "s3"}), 3, 4),
          new TreeDeletionEntry(new MeasurementPath(new String[] {"d1", "s41"}), 4, 5),
          new TableDeletionEntry(new DeletionPredicate("table1", new NOP()), new TimeRange(1, 2)),
          new TableDeletionEntry(
              new DeletionPredicate("table2", new SegmentExactMatch("id11", 0)),
              new TimeRange(3, 4)),
          new TableDeletionEntry(
              new DeletionPredicate(
                  "table3",
                  new FullExactMatch(Factory.DEFAULT_FACTORY.create(new String[] {"id1", "id2"}))),
              new TimeRange(5, 6)),
          new TableDeletionEntry(new DeletionPredicate("table4"), new TimeRange(7, 8)),
        };
    try (ModificationFile mFile = new ModificationFile(tempFileName, false)) {
      for (int i = 0; i < 4; i++) {
        mFile.write(modifications[i]);
      }
      List<ModEntry> modificationList = mFile.getAllMods();
      for (int i = 0; i < 4; i++) {
        assertEquals(modifications[i], modificationList.get(i));
      }

      for (int i = 4; i < 8; i++) {
        mFile.write(modifications[i]);
      }
      modificationList = mFile.getAllMods();
      for (int i = 0; i < 8; i++) {
        assertEquals(modifications[i], modificationList.get(i));
      }
    } catch (IOException e) {
      fail(e.getMessage());
    } finally {
      new File(tempFileName).delete();
    }
  }

  @Test
  public void writeVerifyTest() {
    String tempFileName = TestConstant.BASE_OUTPUT_PATH.concat("mod.temp");
    ModEntry[] modifications =
        new ModEntry[] {
          new TreeDeletionEntry(new MeasurementPath(new String[] {"d1", "s1"}), 1),
          new TreeDeletionEntry(new MeasurementPath(new String[] {"d1", "s2"}), 2),
          new TableDeletionEntry(new DeletionPredicate("table1", new NOP()), new TimeRange(1, 2)),
          new TableDeletionEntry(
              new DeletionPredicate("table2", new SegmentExactMatch("id11", 0)),
              new TimeRange(3, 4)),
          new TableDeletionEntry(
              new DeletionPredicate(
                  "table3",
                  new FullExactMatch(Factory.DEFAULT_FACTORY.create(new String[] {"id1", "id2"}))),
              new TimeRange(5, 6)),
          new TableDeletionEntry(new DeletionPredicate("table4"), new TimeRange(7, 8)),
        };
    try (ModificationFile mFile = new ModificationFile(tempFileName, false)) {
      mFile.write(Arrays.asList(modifications));
      List<ModEntry> modificationList = mFile.getAllMods();
      assertEquals(modifications.length, modificationList.size());
      for (int i = 0; i < modifications.length; i++) {
        assertEquals(modifications[i], modificationList.get(i));
      }
    } catch (IOException e) {
      fail(e.getMessage());
    } finally {
      new File(tempFileName).delete();
    }
  }

  // test if file size greater than 1M.
  @Test
  public void testCompact01() {
    String tempFileName = TestConstant.BASE_OUTPUT_PATH.concat("compact01.mods");
    long time = 1000;
    try (ModificationFile modificationFile = new ModificationFile(tempFileName, false)) {
      while (modificationFile.getFileLength() < 1024 * 1024) {
        modificationFile.write(
            new TreeDeletionEntry(
                new MeasurementPath(new String[] {"root", "sg", "d1"}),
                Long.MIN_VALUE,
                time += 5000));
      }
      modificationFile.compact();
      List<ModEntry> modificationList = new ArrayList<>(modificationFile.getAllMods());
      assertEquals(1, modificationList.size());

      ModEntry deletion = modificationList.get(0);
      assertEquals(time, deletion.getEndTime());
      assertEquals(Long.MIN_VALUE, deletion.getStartTime());
    } catch (IOException e) {
      fail(e.getMessage());
    } finally {
      new File(tempFileName).delete();
    }
  }

  // test if file size less than 1M.
  @Test
  public void testCompact02() {
    String tempFileName = TestConstant.BASE_OUTPUT_PATH.concat("compact02.mods");
    long time = 1000;
    try (ModificationFile modificationFile = new ModificationFile(tempFileName, false)) {
      while (modificationFile.getFileLength() < 1024 * 100) {
        modificationFile.write(
            new TreeDeletionEntry(
                new MeasurementPath(new String[] {"root", "sg", "d1"}),
                Long.MIN_VALUE,
                time += 5000));
      }
      modificationFile.compact();
      List<ModEntry> modificationList = new ArrayList<>(modificationFile.getAllMods());
      assertTrue(modificationList.size() > 1);
    } catch (IOException e) {
      fail(e.getMessage());
    } finally {
      new File(tempFileName).delete();
    }
  }

  // test if file size greater than 1M.
  @Test
  public void testCompact03() {
    String tempFileName = TestConstant.BASE_OUTPUT_PATH.concat("compact03.mods");
    try (ModificationFile modificationFile = new ModificationFile(tempFileName, false)) {
      while (modificationFile.getFileLength() < 1024 * 1024) {
        modificationFile.write(
            new TreeDeletionEntry(
                new MeasurementPath(new String[] {"root", "sg", "d1"}),
                Long.MIN_VALUE,
                Long.MAX_VALUE));
      }
      modificationFile.compact();
      List<ModEntry> modificationList = new ArrayList<>(modificationFile.getAllMods());
      assertEquals(1, modificationList.size());

      ModEntry deletion = modificationList.get(0);
      assertEquals(Long.MAX_VALUE, deletion.getEndTime());
      assertEquals(Long.MIN_VALUE, deletion.getStartTime());
    } catch (IOException e) {
      fail(e.getMessage());
    } finally {
      new File(tempFileName).delete();
    }
  }

  @Test
  public void testCompact04() {
    String tempFileName = TestConstant.BASE_OUTPUT_PATH.concat("compact04.mods");
    try (ModificationFile modificationFile = new ModificationFile(tempFileName, false)) {
      long time = 0;
      while (modificationFile.getFileLength() < 1024 * 1024) {
        for (int i = 0; i < 5; i++) {
          modificationFile.write(
              new TreeDeletionEntry(
                  new MeasurementPath(new String[] {"root", "sg", "d1"}),
                  Long.MIN_VALUE,
                  time += 5000));
          modificationFile.write(
              new TreeDeletionEntry(
                  new MeasurementPath(new String[] {"root", "sg", "*"}),
                  Long.MIN_VALUE,
                  time += 5000));
        }
      }
      modificationFile.compact();
      List<ModEntry> modificationList = new ArrayList<>(modificationFile.getAllMods());
      assertEquals(2, modificationList.size());
    } catch (IOException e) {
      fail(e.getMessage());
    } finally {
      new File(tempFileName).delete();
    }
  }

  @Test
  public void testCompact05() {
    String tempFileName = TestConstant.BASE_OUTPUT_PATH.concat("compact01.mods");
    long time = 1000;
    try (ModificationFile modificationFile = new ModificationFile(tempFileName, false)) {
      while (modificationFile.getFileLength() < 1024 * 1024) {
        modificationFile.write(
            new TableDeletionEntry(
                new DeletionPredicate("table1", new NOP()),
                new TimeRange(Long.MIN_VALUE, time += 5000)));
      }
      modificationFile.compact();
      List<ModEntry> modificationList = new ArrayList<>(modificationFile.getAllMods());
      assertEquals(1, modificationList.size());

      ModEntry deletion = modificationList.get(0);
      assertEquals(time, deletion.getEndTime());
      assertEquals(Long.MIN_VALUE, deletion.getStartTime());
    } catch (IOException e) {
      fail(e.getMessage());
    } finally {
      new File(tempFileName).delete();
    }
  }

  @Test
  public void testCompact06() {
    String tempFileName = TestConstant.BASE_OUTPUT_PATH.concat("compact02.mods");
    long time = 1000;
    try (ModificationFile modificationFile = new ModificationFile(tempFileName, false)) {
      while (modificationFile.getFileLength() < 1024 * 100) {
        modificationFile.write(
            new TableDeletionEntry(
                new DeletionPredicate("table1", new NOP()),
                new TimeRange(Long.MIN_VALUE, time += 5000)));
      }
      modificationFile.compact();
      List<ModEntry> modificationList = new ArrayList<>(modificationFile.getAllMods());
      assertTrue(modificationList.size() > 1);
    } catch (IOException e) {
      fail(e.getMessage());
    } finally {
      new File(tempFileName).delete();
    }
  }

  // test if file size greater than 1M.
  @Test
  public void testCompact07() {
    String tempFileName = TestConstant.BASE_OUTPUT_PATH.concat("compact03.mods");
    try (ModificationFile modificationFile = new ModificationFile(tempFileName, false)) {
      while (modificationFile.getFileLength() < 1024 * 1024) {
        modificationFile.write(
            new TableDeletionEntry(
                new DeletionPredicate("table1", new NOP()),
                new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE)));
      }
      modificationFile.compact();
      List<ModEntry> modificationList = new ArrayList<>(modificationFile.getAllMods());
      assertEquals(1, modificationList.size());

      ModEntry deletion = modificationList.get(0);
      assertEquals(Long.MAX_VALUE, deletion.getEndTime());
      assertEquals(Long.MIN_VALUE, deletion.getStartTime());
    } catch (IOException e) {
      fail(e.getMessage());
    } finally {
      new File(tempFileName).delete();
    }
  }

  @Test
  public void testCompact08() {
    String tempFileName = TestConstant.BASE_OUTPUT_PATH.concat("compact04.mods");
    try (ModificationFile modificationFile = new ModificationFile(tempFileName, false)) {
      long time = 0;
      while (modificationFile.getFileLength() < 1024 * 1024) {
        for (int i = 0; i < 5; i++) {
          ModEntry[] modEntries = {
            new TableDeletionEntry(
                new DeletionPredicate("table1", new NOP()),
                new TimeRange(Long.MIN_VALUE, time += 5000)),
            new TableDeletionEntry(
                new DeletionPredicate("table2", new SegmentExactMatch("id11", 0)),
                new TimeRange(Long.MIN_VALUE, time += 5000)),
            new TableDeletionEntry(
                new DeletionPredicate(
                    "table3",
                    new FullExactMatch(
                        Factory.DEFAULT_FACTORY.create(new String[] {"id1", "id2"}))),
                new TimeRange(Long.MIN_VALUE, time += 5000)),
            new TableDeletionEntry(
                new DeletionPredicate("table4"), new TimeRange(Long.MIN_VALUE, time += 5000))
          };
          modificationFile.write(Arrays.asList(modEntries));
        }
      }
      modificationFile.compact();
      List<ModEntry> modificationList = new ArrayList<>(modificationFile.getAllMods());
      assertEquals(4, modificationList.size());
    } catch (IOException e) {
      fail(e.getMessage());
    } finally {
      new File(tempFileName).delete();
    }
  }

  // test mods file and mods settle file both exists
  @Test
  public void testRecover01() throws IOException {
    String modsFileName = TestConstant.BASE_OUTPUT_PATH.concat("compact01.mods");
    String modsSettleFileName = TestConstant.BASE_OUTPUT_PATH.concat("compact01.mods.settle");

    try (ModificationFile modsFile = new ModificationFile(modsFileName, false);
        ModificationFile modsSettleFile = new ModificationFile(modsSettleFileName, false)) {

      modsFile.write(
          new TreeDeletionEntry(
              new MeasurementPath(new String[] {"root", "sg", "d1"}),
              Long.MIN_VALUE,
              Long.MAX_VALUE));
      modsSettleFile.write(
          new TreeDeletionEntry(
              new MeasurementPath(new String[] {"root", "sg", "d1"}),
              Long.MIN_VALUE,
              Long.MAX_VALUE));

      modsFile.close();
      modsSettleFile.close();
      new CompactionRecoverManager(null, null, null)
          .recoverModSettleFile(new File(TestConstant.BASE_OUTPUT_PATH).toPath());
      Assert.assertTrue(modsFile.exists());
      Assert.assertFalse(modsSettleFile.getFileLength() > 0);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      Files.delete(new File(modsFileName).toPath());
    }
  }

  // test only mods settle file exists
  @Test
  public void testRecover02() throws IOException {
    String modsSettleFileName = TestConstant.BASE_OUTPUT_PATH.concat("compact02.mods.settle");
    String originModsFileName = TestConstant.BASE_OUTPUT_PATH.concat("compact02.mods");
    try (ModificationFile modsSettleFile = new ModificationFile(modsSettleFileName, false)) {
      modsSettleFile.write(
          new TreeDeletionEntry(
              new MeasurementPath(new String[] {"root", "sg", "d1"}),
              Long.MIN_VALUE,
              Long.MAX_VALUE));
      modsSettleFile.close();
      new CompactionRecoverManager(null, null, null)
          .recoverModSettleFile(new File(TestConstant.BASE_OUTPUT_PATH).toPath());
      Assert.assertFalse(modsSettleFile.getFileLength() > 0);
      Assert.assertTrue(new File(originModsFileName).exists());
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      Files.delete(new File(originModsFileName).toPath());
    }
  }

  @Test
  public void testRecover03() throws IOException {
    String modsFileName = TestConstant.BASE_OUTPUT_PATH.concat("compact01.mods");
    String modsSettleFileName = TestConstant.BASE_OUTPUT_PATH.concat("compact01.mods.settle");

    long time = 0;
    try (ModificationFile modsFile = new ModificationFile(modsFileName, false);
        ModificationFile modsSettleFile = new ModificationFile(modsSettleFileName, false)) {

      ModEntry[] modEntries = {
        new TableDeletionEntry(
            new DeletionPredicate("table1", new NOP()),
            new TimeRange(Long.MIN_VALUE, time += 5000)),
        new TableDeletionEntry(
            new DeletionPredicate("table2", new SegmentExactMatch("id11", 0)),
            new TimeRange(Long.MIN_VALUE, time += 5000)),
        new TableDeletionEntry(
            new DeletionPredicate(
                "table3",
                new FullExactMatch(Factory.DEFAULT_FACTORY.create(new String[] {"id1", "id2"}))),
            new TimeRange(Long.MIN_VALUE, time += 5000)),
        new TableDeletionEntry(
            new DeletionPredicate("table4"), new TimeRange(Long.MIN_VALUE, time += 5000))
      };
      modsFile.write(Arrays.asList(modEntries));

      modsFile.close();
      modsSettleFile.close();
      new CompactionRecoverManager(null, null, null)
          .recoverModSettleFile(new File(TestConstant.BASE_OUTPUT_PATH).toPath());
      Assert.assertTrue(modsFile.exists());
      Assert.assertFalse(modsSettleFile.getFileLength() > 0);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      Files.delete(new File(modsFileName).toPath());
    }
  }

  @Test
  public void testRecover04() throws IOException {
    String modsSettleFileName = TestConstant.BASE_OUTPUT_PATH.concat("compact02.mods.settle");
    String originModsFileName = TestConstant.BASE_OUTPUT_PATH.concat("compact02.mods");

    long time = 0;
    try (ModificationFile modsSettleFile = new ModificationFile(modsSettleFileName, false)) {
      ModEntry[] modEntries = {
        new TableDeletionEntry(
            new DeletionPredicate("table1", new NOP()),
            new TimeRange(Long.MIN_VALUE, time += 5000)),
        new TableDeletionEntry(
            new DeletionPredicate("table2", new SegmentExactMatch("id11", 0)),
            new TimeRange(Long.MIN_VALUE, time += 5000)),
        new TableDeletionEntry(
            new DeletionPredicate(
                "table3",
                new FullExactMatch(Factory.DEFAULT_FACTORY.create(new String[] {"id1", "id2"}))),
            new TimeRange(Long.MIN_VALUE, time += 5000)),
        new TableDeletionEntry(
            new DeletionPredicate("table4"), new TimeRange(Long.MIN_VALUE, time += 5000))
      };
      modsSettleFile.write(Arrays.asList(modEntries));
      modsSettleFile.close();
      new CompactionRecoverManager(null, null, null)
          .recoverModSettleFile(new File(TestConstant.BASE_OUTPUT_PATH).toPath());
      Assert.assertFalse(modsSettleFile.getFileLength() > 0);
      Assert.assertTrue(new File(originModsFileName).exists());
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      Files.delete(new File(originModsFileName).toPath());
    }
  }

  @Test
  public void testConcurrentClose() throws ExecutionException, InterruptedException, IOException {
    String modsFileName = TestConstant.BASE_OUTPUT_PATH.concat("concurrentClose.mods");
    try (ModificationFile modsFile = new ModificationFile(modsFileName, false)) {
      ExecutorService threadPool = Executors.newCachedThreadPool();
      AtomicInteger writeCounter = new AtomicInteger();
      int maxWrite = 10000;
      int closeInterval = 100;
      int closeFutureNum = 5;
      Future<Void> writeFuture = threadPool.submit(() -> write(modsFile, maxWrite, writeCounter));
      List<Future<Void>> closeFutures = new ArrayList<>();
      for (int i = 0; i < closeFutureNum; i++) {
        closeFutures.add(
            threadPool.submit(() -> close(modsFile, writeCounter, maxWrite, closeInterval)));
      }
      writeFuture.get();
      for (Future<Void> closeFuture : closeFutures) {
        closeFuture.get();
      }
      assertEquals(maxWrite, modsFile.getAllMods().size());
    } finally {
      Files.delete(new File(modsFileName).toPath());
    }
  }

  private Void write(ModificationFile modificationFile, int maxWrite, AtomicInteger writeCounter)
      throws IllegalPathException, IOException {
    for (int i = 0; i < maxWrite; i++) {
      modificationFile.write(new TreeDeletionEntry(new MeasurementPath("root.db1.d1.s1"), i, i));
      writeCounter.incrementAndGet();
    }
    return null;
  }

  private Void close(
      ModificationFile modificationFile,
      AtomicInteger writeCounter,
      int maxWrite,
      int closeInterval)
      throws IOException, InterruptedException {
    int prevWriteCnt = 0;
    while (writeCounter.get() < maxWrite) {
      int writeCnt = writeCounter.get();
      if (writeCnt - prevWriteCnt >= closeInterval) {
        modificationFile.close();
        prevWriteCnt = writeCnt;
      }
      Thread.sleep(10);
    }
    return null;
  }
}
