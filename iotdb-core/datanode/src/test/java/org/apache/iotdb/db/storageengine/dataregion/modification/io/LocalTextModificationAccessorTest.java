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

package org.apache.iotdb.db.storageengine.dataregion.modification.io;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.junit.AfterClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class LocalTextModificationAccessorTest {
  private static Modification[] modifications =
      new Modification[] {
        new Deletion(new MeasurementPath(new String[] {"d1", "s1"}), 1, 1),
        new Deletion(new MeasurementPath(new String[] {"d1", "s2"}), 2, 2),
        new Deletion(new MeasurementPath(new String[] {"d1", "s3"}), 3, 3),
        new Deletion(new MeasurementPath(new String[] {"d1", "s4"}), 4, 4),
      };

  @AfterClass
  public static void tearDown() {
    modifications = null;
  }

  @Test
  public void writeMeetException() throws IOException {
    String tempFileName = TestConstant.BASE_OUTPUT_PATH.concat("mod.temp");
    long length = 0;
    LocalTextModificationAccessor accessor = null;
    try {
      accessor = new LocalTextModificationAccessor(tempFileName);
      for (int i = 0; i < 2; i++) {
        accessor.write(modifications[i]);
      }
      length = new File(tempFileName).length();
      // the current line should be truncated when meet exception
      accessor.writeMeetException(modifications[2]);
      for (int i = 2; i < 4; i++) {
        accessor.write(modifications[i]);
      }
      List<Modification> modificationList = (List<Modification>) accessor.read();
      assertEquals(4, modificationList.size());
      for (int i = 0; i < 4; i++) {
        assertEquals(modifications[i], modificationList.get(i));
      }
    } catch (IOException e) {
      accessor.truncate(length);
    } finally {
      if (accessor != null) {
        accessor.close();
      }
      new File(tempFileName).delete();
    }
  }

  @Test
  public void readMyWrite() {
    String tempFileName = TestConstant.BASE_OUTPUT_PATH.concat("mod.temp");
    try (LocalTextModificationAccessor accessor = new LocalTextModificationAccessor(tempFileName)) {
      for (int i = 0; i < 2; i++) {
        accessor.write(modifications[i]);
      }
      List<Modification> modificationList = (List<Modification>) accessor.read();
      for (int i = 0; i < 2; i++) {
        assertEquals(modifications[i], modificationList.get(i));
      }

      for (int i = 2; i < 4; i++) {
        accessor.write(modifications[i]);
      }
      modificationList = (List<Modification>) accessor.read();
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
  public void readNull() {
    String tempFileName = TestConstant.BASE_OUTPUT_PATH.concat("mod.temp");
    try (LocalTextModificationAccessor accessor = new LocalTextModificationAccessor(tempFileName)) {
      new File(tempFileName).delete();
      Collection<Modification> modifications = accessor.read();
      assertEquals(new ArrayList<>(), modifications);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void readMeetError() {
    String tempFileName = TestConstant.BASE_OUTPUT_PATH.concat("mod.temp");
    File file = new File(tempFileName);
    if (file.exists()) {
      file.delete();
    }
    try (LocalTextModificationAccessor accessor = new LocalTextModificationAccessor(tempFileName)) {
      // write normal message
      for (int i = 0; i < 4; i++) {
        accessor.write(modifications[i]);
      }
      List<Modification> modificationList = (List<Modification>) accessor.read();
      for (int i = 0; i < 4; i++) {
        assertEquals(modifications[i], modificationList.get(i));
      }
      // write error message
      accessor.writeInComplete(modifications[0]);

      modificationList = (List<Modification>) accessor.read();
      // the error line is ignored
      assertEquals(4, modificationList.size());
      for (int i = 0; i < 4; i++) {
        System.out.println(modificationList);
        assertEquals(modifications[i], modificationList.get(i));
      }
    } catch (IOException e) {
      fail(e.getMessage());
    } finally {
      file.delete();
    }
  }
}
