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

import org.apache.iotdb.cli.fs.path.FsPath;

import java.util.ArrayList;
import java.util.List;

public final class TreeFilesystemSql {

  private TreeFilesystemSql() {}

  public static String toTreePath(FsPath path) {
    StringBuilder builder = new StringBuilder();
    for (String segment : path.getSegments()) {
      if (builder.length() > 0) {
        builder.append('.');
      }
      builder.append(segment);
    }
    return builder.toString();
  }

  public static FsPath fromTreePath(String treePath) {
    if (treePath == null || treePath.isEmpty()) {
      return FsPath.absolute("/");
    }
    return FsPath.absolute("/" + String.join("/", splitTreePath(treePath)));
  }

  public static String measurementName(String timeseries) {
    List<String> segments = splitTreePath(timeseries);
    return segments.isEmpty() ? timeseries : segments.get(segments.size() - 1);
  }

  public static String parentTreePath(String treePath) {
    List<String> segments = splitTreePath(treePath);
    if (segments.size() <= 1) {
      return "";
    }
    return String.join(".", segments.subList(0, segments.size() - 1));
  }

  public static FsPath parent(FsPath path) {
    List<String> segments = path.getSegments();
    StringBuilder builder = new StringBuilder("/");
    for (int i = 0; i < segments.size() - 1; i++) {
      if (i > 0) {
        builder.append('/');
      }
      builder.append(segments.get(i));
    }
    return FsPath.absolute(builder.toString());
  }

  private static List<String> splitTreePath(String treePath) {
    List<String> segments = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    boolean quoted = false;
    for (int i = 0; i < treePath.length(); i++) {
      char c = treePath.charAt(i);
      if (c == '`') {
        quoted = !quoted;
        current.append(c);
      } else if (c == '.' && !quoted) {
        segments.add(current.toString());
        current.setLength(0);
      } else {
        current.append(c);
      }
    }
    segments.add(current.toString());
    return segments;
  }
}
