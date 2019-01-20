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

package org.apache.iotdb.db.engine.version;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import static org.apache.iotdb.db.engine.version.SimpleFileVersionController.SAVE_INTERVAL;
import static org.junit.Assert.assertEquals;

public class SimpleFileVersionControllerTest {
  @Test
  public void test() throws IOException {
    String tempFilePath = "version.tmp";

    try {
      new File(tempFilePath).mkdir();
      VersionController versionController = new SimpleFileVersionController(tempFilePath);
      assertEquals(versionController.currVersion(), SAVE_INTERVAL);
      for (int i = 0; i < 150; i++) {
        versionController.nextVersion();
      }
      assertEquals(versionController.currVersion(), SAVE_INTERVAL + 150);
      versionController = new SimpleFileVersionController(tempFilePath);
      assertEquals(versionController.currVersion(), SAVE_INTERVAL + 200);
    } finally {
      FileUtils.deleteDirectory(new File(tempFilePath));
    }
  }
}