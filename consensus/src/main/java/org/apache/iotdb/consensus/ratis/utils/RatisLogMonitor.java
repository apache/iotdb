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
package org.apache.iotdb.consensus.ratis.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Monitoring Ratis RaftLog total size. It will memorize the state of all files in the last update
 * and calculates the diff incrementally each run.
 */
public class RatisLogMonitor {
  private static final Logger logger = LoggerFactory.getLogger(RatisLogMonitor.class);

  /* whether the path denotes an open segment under active writing progress */
  private static final Predicate<Path> isOpenSegment =
      p -> p.toFile().getName().startsWith("log_inprogress");

  private static final class DirectoryState {
    private long size = 0;
    private Set<Path> memorizedFiles = Collections.emptySet();

    private void update(long size, Set<Path> latest) {
      this.size = size;
      this.memorizedFiles = latest;
    }
  }

  private final HashMap<File, DirectoryState> directoryMap = new HashMap<>();

  public long updateAndGetDirectorySize(File dir) {
    final DirectoryState state = directoryMap.computeIfAbsent(dir, d -> new DirectoryState());
    Set<Path> latest;
    try (final Stream<Path> files = Files.list(dir.toPath())) {
      latest = files.filter(isOpenSegment).collect(Collectors.toSet());
    } catch (IOException e) {
      logger.warn("{}: Error caught when listing files under {}:", this, dir, e);
      // keep the files unchanged and return the size calculated last time
      return state.size;
    }
    final long sizeDiff = diff(state.memorizedFiles, latest);
    final long newSize = state.size + sizeDiff;
    state.update(newSize, latest);
    return newSize;
  }

  public Set<Path> getFilesUnder(File dir) {
    return Collections.unmodifiableSet(directoryMap.get(dir).memorizedFiles);
  }

  private static long diff(Set<Path> old, Set<Path> latest) {
    final long incremental = totalSize(latest.stream().filter(p -> !old.contains(p)));
    final long decremental = totalSize(old.stream().filter(p -> !latest.contains(p)));
    return incremental - decremental;
  }

  private static long totalSize(Stream<Path> files) {
    return files.mapToLong(p -> p.toFile().length()).sum();
  }
}
