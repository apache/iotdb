/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.engine.modification;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ModificationFileTest {
  @Test
  public void readMyWrite() {
    String tempFileName = "mod.temp";
    Modification[] modifications = new Modification[]{
            new Deletion("p1", 1, 1),
            new Deletion("p2", 2, 2),
            new Deletion("p3", 3, 3),
            new Deletion("p4", 4, 4),
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
}