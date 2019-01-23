/**
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
package org.apache.iotdb.db.conf.directories.strategy;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MaxDiskUsableSpaceFirstStrategy extends DirectoryStrategy {

  // disk space is measured by MB
  private static final double DATA_SIZE_SHIFT = 1024D * 1024;

  @Override
  public int nextFolderIndex() {
    return getMaxSpaceFolder();
  }

  /**
   * get max space folder.
   */
  public int getMaxSpaceFolder() {
    List<Integer> candidates = new ArrayList<>();
    long max;

    candidates.add(0);
    max = getUsableSpace(folders.get(0));
    for (int i = 1; i < folders.size(); i++) {
      long current = getUsableSpace(folders.get(i));
      if (max < current) {
        candidates.clear();
        candidates.add(i);
        max = current;
      } else if (max == current) {
        candidates.add(i);
      }
    }

    Random random = new Random(System.currentTimeMillis());
    int index = random.nextInt(candidates.size());

    return candidates.get(index);
  }

  private long getUsableSpace(String path) {
    double freespace = new File(path).getUsableSpace() / DATA_SIZE_SHIFT;
    return (long) freespace;
  }
}
