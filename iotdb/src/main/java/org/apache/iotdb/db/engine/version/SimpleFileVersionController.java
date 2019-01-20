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
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * SimpleFileVersionController uses a local file and its file name to store the version.
 */
public class SimpleFileVersionController implements VersionController {

  /**
   * Every time currVersion - prevVersion >= SAVE_INTERVAL, currVersion is persisted and prevVersion
   * is set to currVersion. When recovering from file, the version number is automatically increased
   * by SAVE_INTERVAL to avoid conflicts.
   */
  public static final long SAVE_INTERVAL = 100;
  private static final String FILE_PREFIX = "Version-";
  private long prevVersion;
  private long currVersion;
  private String directoryPath;

  public SimpleFileVersionController(String directoryPath) throws IOException {
    this.directoryPath = directoryPath;
    restore();
  }

  @Override
  public synchronized long nextVersion() {
    currVersion ++;
    checkPersist();
    return currVersion;
  }

  /**
   * Test only method, no need for concurrency.
   * @return the current version.
   */
  @Override
  public long currVersion() {
    return currVersion;
  }

  private void checkPersist() {
    if ((currVersion - prevVersion) >= SAVE_INTERVAL) {
      persist();
    }
  }

  private void persist() {
    File oldFile = new File(directoryPath,FILE_PREFIX + prevVersion);
    File newFile = new File(directoryPath, FILE_PREFIX + currVersion);
    oldFile.renameTo(newFile);
    prevVersion = currVersion;
  }

  private void restore() throws IOException {
    File directory = new File(directoryPath);
    File[] versionFiles = directory.listFiles((dir, name) -> name.startsWith(FILE_PREFIX));
    File versionFile = null;
    if (versionFiles != null && versionFiles.length > 0) {
      Arrays.sort(versionFiles, Comparator.comparing(File::getName));
      versionFile = versionFiles[versionFiles.length - 1];
      for(int i = 0; i < versionFiles.length - 1; i ++) {
        versionFiles[i].delete();
      }
    } else {
      versionFile = new File(directory, FILE_PREFIX + "0");
      new FileOutputStream(versionFile).close();
    }
    // extract version from "Version-123456"
    prevVersion = Long.parseLong(versionFile.getName().split("-")[1]);
    // prevent overlapping in case of failure
    currVersion = prevVersion + SAVE_INTERVAL;
    persist();
  }
}
