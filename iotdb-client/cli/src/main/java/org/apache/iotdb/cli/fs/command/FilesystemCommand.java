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

package org.apache.iotdb.cli.fs.command;

import java.util.Collections;
import java.util.List;

public class FilesystemCommand {

  public enum Type {
    PWD,
    LS,
    LL,
    CD,
    STAT,
    CAT,
    PASTE,
    TREE,
    SQL,
    HELP,
    EXIT,
    INVALID
  }

  private final Type type;
  private final String path;
  private final List<String> paths;
  private final int depth;
  private final String statement;
  private final String errorMessage;

  private FilesystemCommand(
      Type type,
      String path,
      List<String> paths,
      int depth,
      String statement,
      String errorMessage) {
    this.type = type;
    this.path = path;
    this.paths = paths;
    this.depth = depth;
    this.statement = statement;
    this.errorMessage = errorMessage;
  }

  public static FilesystemCommand simple(Type type) {
    return new FilesystemCommand(type, "", Collections.emptyList(), -1, "", "");
  }

  public static FilesystemCommand path(Type type, String path) {
    return new FilesystemCommand(type, path, Collections.singletonList(path), -1, "", "");
  }

  public static FilesystemCommand paths(Type type, List<String> paths) {
    String path = paths.isEmpty() ? "" : paths.get(0);
    return new FilesystemCommand(type, path, Collections.unmodifiableList(paths), -1, "", "");
  }

  public static FilesystemCommand tree(String path, int depth) {
    return new FilesystemCommand(Type.TREE, path, Collections.singletonList(path), depth, "", "");
  }

  public static FilesystemCommand sql(String statement) {
    return new FilesystemCommand(Type.SQL, "", Collections.emptyList(), -1, statement, "");
  }

  public static FilesystemCommand invalid(String errorMessage) {
    return new FilesystemCommand(Type.INVALID, "", Collections.emptyList(), -1, "", errorMessage);
  }

  public Type getType() {
    return type;
  }

  public String getPath() {
    return path;
  }

  public List<String> getPaths() {
    return paths;
  }

  public int getDepth() {
    return depth;
  }

  public String getStatement() {
    return statement;
  }

  public String getErrorMessage() {
    return errorMessage;
  }
}
