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

package org.apache.iotdb.cli.fs.provider;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/** Exact external merge selection: bounded value buffer and at most 32 open runs. */
final class ExactMedian implements AutoCloseable {
  private static final int BUFFER = 4096;
  private static final int FAN_IN = 32;
  private final Comparator<String> comparator;
  private final List<String> values = new ArrayList<>();
  private final List<List<Path>> levels = new ArrayList<>();
  private final List<Path> owned = new ArrayList<>();
  private long count;

  ExactMedian(Comparator<String> comparator) {
    this.comparator = comparator;
  }

  void add(String value) {
    values.add(value);
    count++;
    if (values.size() == BUFFER) flush();
  }

  private Path temporary() throws IOException {
    Path path = Files.createTempFile("iotdb-fs-median-", ".run");
    owned.add(path);
    return path;
  }

  private void flush() {
    if (values.isEmpty()) return;
    try {
      values.sort(comparator);
      Path path = temporary();
      try (BufferedWriter out = Files.newBufferedWriter(path)) {
        for (String value : values) {
          out.write(value);
          out.newLine();
        }
      }
      values.clear();
      int level = 0;
      while (true) {
        if (levels.size() == level) levels.add(new ArrayList<>());
        List<Path> runs = levels.get(level);
        runs.add(path);
        if (runs.size() < FAN_IN) break;
        path = merge(runs);
        runs.clear();
        level++;
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Path merge(List<Path> runs) throws IOException {
    Path result = temporary();
    try (MergeReader in = new MergeReader(runs);
        BufferedWriter out = Files.newBufferedWriter(result)) {
      String value;
      while ((value = in.next()) != null) {
        out.write(value);
        out.newLine();
      }
    }
    for (Path path : runs) {
      Files.delete(path);
      owned.remove(path);
    }
    return result;
  }

  String[] middle() {
    if (count == 0) return null;
    if (owned.isEmpty()) {
      values.sort(comparator);
      return new String[] {values.get((values.size() - 1) / 2), values.get(values.size() / 2)};
    }
    flush();
    try {
      List<Path> runs = new ArrayList<>();
      for (List<Path> level : levels) runs.addAll(level);
      while (runs.size() > FAN_IN) {
        List<Path> next = new ArrayList<>();
        for (int from = 0; from < runs.size(); from += FAN_IN) {
          next.add(merge(runs.subList(from, Math.min(from + FAN_IN, runs.size()))));
        }
        runs = next;
      }
      try (MergeReader in = new MergeReader(runs)) {
        String lower = null, upper = null;
        for (long i = 0; i <= count / 2; i++) {
          String value = in.next();
          if (i == (count - 1) / 2) lower = value;
          if (i == count / 2) upper = value;
        }
        return new String[] {lower, upper};
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void close() {
    IOException failure = null;
    for (Path path : owned) {
      try {
        Files.deleteIfExists(path);
      } catch (IOException e) {
        if (failure == null) failure = e;
        else failure.addSuppressed(e);
      }
    }
    owned.clear();
    values.clear();
    if (failure != null) throw new UncheckedIOException(failure);
  }

  private final class MergeReader implements AutoCloseable {
    private final List<BufferedReader> readers = new ArrayList<>();
    private final PriorityQueue<Entry> queue =
        new PriorityQueue<>((a, b) -> comparator.compare(a.value, b.value));

    MergeReader(List<Path> paths) throws IOException {
      try {
        for (Path path : paths) {
          BufferedReader reader = Files.newBufferedReader(path);
          readers.add(reader);
          String value = reader.readLine();
          if (value != null) queue.add(new Entry(reader, value));
        }
      } catch (IOException e) {
        close();
        throw e;
      }
    }

    String next() throws IOException {
      Entry entry = queue.poll();
      if (entry == null) return null;
      String value = entry.value;
      entry.value = entry.reader.readLine();
      if (entry.value != null) queue.add(entry);
      return value;
    }

    @Override
    public void close() throws IOException {
      IOException failure = null;
      for (BufferedReader reader : readers) {
        try {
          reader.close();
        } catch (IOException e) {
          if (failure == null) failure = e;
          else failure.addSuppressed(e);
        }
      }
      if (failure != null) throw failure;
    }
  }

  private static final class Entry {
    private final BufferedReader reader;
    private String value;

    private Entry(BufferedReader reader, String value) {
      this.reader = reader;
      this.value = value;
    }
  }
}
