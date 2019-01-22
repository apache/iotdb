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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MinDirOccupiedSpaceFirstStrategy extends DirectoryStrategy {

  // directory space is measured by MB
  private final long dataSizeShift = 1024 * 1024;

  @Override
  public int nextFolderIndex() {
    return getMinOccupiedSpaceFolder();
  }

  private int getMinOccupiedSpaceFolder() {
    List<Integer> candidates = new ArrayList<>();
    long min = 0;

    candidates.add(0);
    min = getOccupiedSpace(folders.get(0));
    for (int i = 1; i < folders.size(); i++) {
      long current = getOccupiedSpace(folders.get(i));
      if (min > current) {
        candidates.clear();
        candidates.add(i);
        min = current;
      } else if (min == current) {
        candidates.add(i);
      }
    }

    Random random = new Random(System.currentTimeMillis());
    int index = random.nextInt(candidates.size());

    return candidates.get(index);
  }

  private long getOccupiedSpace(String path) {
    Path folder = Paths.get(path);
    long size = 0;
    try {
      size = Files.walk(folder).filter(p -> p.toFile().isFile()).mapToLong(p -> p.toFile().length())
          .sum();
    } catch (IOException e) {
      LOGGER.error("Cannot calculate occupied space for seriesPath {}.", path);
    }

    return size / dataSizeShift;
  }
}
