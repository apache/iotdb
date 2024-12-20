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

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.recover.CompactionRecoverManager;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

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
          new TreeDeletionEntry(new MeasurementPath(new String[] {"d1", "s41"}), 4, 5)
        };
    try (ModificationFile mFile = new ModificationFile(tempFileName)) {
      for (int i = 0; i < 2; i++) {
        mFile.write(modifications[i]);
      }
      List<ModEntry> modificationList = mFile.getAllMods();
      for (int i = 0; i < 2; i++) {
        assertEquals(modifications[i], modificationList.get(i));
      }

      for (int i = 2; i < 4; i++) {
        mFile.write(modifications[i]);
      }
      modificationList = mFile.getAllMods();
      for (int i = 0; i < 4; i++) {
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
          new TreeDeletionEntry(new MeasurementPath(new String[] {"d1", "s2"}), 2)
        };
    try (ModificationFile mFile = new ModificationFile(tempFileName)) {
      mFile.write(modifications[0]);
      mFile.write(modifications[1]);
      List<ModEntry> modificationList = mFile.getAllMods();
      assertEquals(2, modificationList.size());
      for (int i = 0; i < 2; i++) {
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
    try (ModificationFile modificationFile = new ModificationFile(tempFileName)) {
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
    try (ModificationFile modificationFile = new ModificationFile(tempFileName)) {
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
    try (ModificationFile modificationFile = new ModificationFile(tempFileName)) {
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
    try (ModificationFile modificationFile = new ModificationFile(tempFileName)) {
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

  // test mods file and mods settle file both exists
  @Test
  public void testRecover01() {
    String modsFileName = TestConstant.BASE_OUTPUT_PATH.concat("compact01.mods");
    String modsSettleFileName = TestConstant.BASE_OUTPUT_PATH.concat("compact01.mods.settle");

    try (ModificationFile modsFile = new ModificationFile(modsFileName);
        ModificationFile modsSettleFile = new ModificationFile(modsSettleFileName)) {

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
      try {
        Files.delete(new File(modsFileName).toPath());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  // test only mods settle file exists
  @Test
  public void testRecover02() {
    String modsSettleFileName = TestConstant.BASE_OUTPUT_PATH.concat("compact02.mods.settle");
    String originModsFileName = TestConstant.BASE_OUTPUT_PATH.concat("compact02.mods");
    try (ModificationFile modsSettleFile = new ModificationFile(modsSettleFileName)) {
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
      try {
        Files.delete(new File(originModsFileName).toPath());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
