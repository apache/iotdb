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

package org.apache.iotdb.cli.fs.path;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Objects;

public class FsPath {

  private static final String SEPARATOR = "/";

  private final List<String> segments;

  private FsPath(List<String> segments) {
    this.segments = Collections.unmodifiableList(new ArrayList<>(segments));
  }

  public static FsPath absolute(String path) {
    return new FsPath(normalize(path));
  }

  public FsPath resolve(String path) {
    if (path == null || path.isEmpty()) {
      return this;
    }
    if (path.startsWith(SEPARATOR)) {
      return absolute(path);
    }

    List<String> combined = new ArrayList<>(segments);
    combined.addAll(split(path));
    return new FsPath(normalize(combined));
  }

  public boolean isRoot() {
    return segments.isEmpty();
  }

  public List<String> getSegments() {
    return segments;
  }

  public String getFileName() {
    if (segments.isEmpty()) {
      return "";
    }
    return segments.get(segments.size() - 1);
  }

  @Override
  public String toString() {
    if (segments.isEmpty()) {
      return SEPARATOR;
    }
    return SEPARATOR + String.join(SEPARATOR, segments);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FsPath)) {
      return false;
    }
    FsPath fsPath = (FsPath) o;
    return Objects.equals(segments, fsPath.segments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(segments);
  }

  private static List<String> normalize(String path) {
    return normalize(split(path));
  }

  private static List<String> normalize(List<String> pathSegments) {
    Deque<String> normalized = new ArrayDeque<>();
    for (String segment : pathSegments) {
      if (segment.isEmpty() || ".".equals(segment)) {
        continue;
      }
      if ("..".equals(segment)) {
        if (!normalized.isEmpty()) {
          normalized.removeLast();
        }
        continue;
      }
      normalized.addLast(segment);
    }
    return new ArrayList<>(normalized);
  }

  private static List<String> split(String path) {
    if (path == null || path.isEmpty()) {
      return Collections.emptyList();
    }
    List<String> result = new ArrayList<>();
    for (String segment : path.split(SEPARATOR)) {
      result.add(segment);
    }
    return result;
  }
}
