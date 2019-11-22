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

package org.apache.iotdb.db.engine.modification;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.Ignore;
import org.junit.Test;

public class ModificationFileTest {

  @Test
  public void readMyWrite() {
    String tempFileName = "target/mod.temp";
    Modification[] modifications = new Modification[]{
        new Deletion(new Path("d1", "s1"), 1, 1),
        new Deletion(new Path("d1", "s2"), 2, 2),
        new Deletion(new Path("d1", "s3"), 3, 3),
        new Deletion(new Path("d1", "s41"), 4, 4)
    };
    try {
      ModificationFile mFile = new ModificationFile(tempFileName);
      for (int i = 0; i < 2; i++) {
        mFile.write(modifications[i]);
      }
      List<Modification> modificationList = (List<Modification>) mFile.getModifications();
      for (int i = 0; i < 2; i++) {
        assertEquals(modifications[i], modificationList.get(i));
      }

      for (int i = 2; i < 4; i++) {
        mFile.write(modifications[i]);
      }
      modificationList = (List<Modification>) mFile.getModifications();
      for (int i = 0; i < 4; i++) {
        assertEquals(modifications[i], modificationList.get(i));
      }
      mFile.close();
    } catch (IOException e) {
      fail(e.getMessage());
    } finally {
      new File(tempFileName).delete();
    }
  }

  @Test
  public void testAbort() {
    String tempFileName = "target/mod.temp";
    Modification[] modifications = new Modification[]{
        new Deletion(new Path("d1", "s1"), 1, 1),
        new Deletion(new Path("d1", "s2"), 2, 2),
        new Deletion(new Path("d1", "s3"), 3, 3),
        new Deletion(new Path("d1", "s41"), 4, 4),
    };
    try {
      ModificationFile mFile = new ModificationFile(tempFileName);
      for (int i = 0; i < 2; i++) {
        mFile.write(modifications[i]);
      }
      List<Modification> modificationList = (List<Modification>) mFile.getModifications();
      for (int i = 0; i < 2; i++) {
        assertEquals(modifications[i], modificationList.get(i));
      }

      for (int i = 2; i < 4; i++) {
        mFile.write(modifications[i]);
      }
      modificationList = (List<Modification>) mFile.getModifications();
      mFile.abort();

      for (int i = 0; i < 3; i++) {
        assertEquals(modifications[i], modificationList.get(i));
      }
      mFile.close();
    } catch (IOException e) {
      fail(e.getMessage());
    } finally {
      new File(tempFileName).delete();
    }
  }
}