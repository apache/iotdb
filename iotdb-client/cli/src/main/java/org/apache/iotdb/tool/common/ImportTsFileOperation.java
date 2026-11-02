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

package org.apache.iotdb.tool.common;

import org.apache.iotdb.cli.utils.IoTPrinter;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public enum ImportTsFileOperation {
  NONE,
  MV,
  HARDLINK,
  CP,
  DELETE,
  ;

  /** Process a TsFile together with its sidecars without replacing existing archive files. */
  public void processFileGroup(Path source, Path target) throws IOException {
    List<String> suffixes = new ArrayList<>();
    suffixes.add("");
    for (String suffix : new String[] {".resource", ".mods"}) {
      if (Files.exists(Paths.get(source + suffix))) {
        suffixes.add(suffix);
      }
    }
    if (this == CP || this == HARDLINK || this == MV) {
      // Check every destination before moving any file in the TsFile/sidecar group.
      for (String suffix : suffixes) {
        Path destination = Paths.get(target + suffix);
        if (Files.exists(destination, LinkOption.NOFOLLOW_LINKS)) {
          throw new FileAlreadyExistsException(destination.toString());
        }
      }
      Files.createDirectories(target.getParent());
    }
    for (String suffix : suffixes) {
      ImportTsFileOperation operation = this == HARDLINK && !suffix.isEmpty() ? CP : this;
      operation.processFile(Paths.get(source + suffix), Paths.get(target + suffix));
    }
  }

  private void processFile(Path source, Path target) throws IOException {
    switch (this) {
      case DELETE:
        Files.deleteIfExists(source);
        break;
      case CP:
        Files.copy(source, target);
        break;
      case HARDLINK:
        try {
          Files.createLink(target, source);
        } catch (FileAlreadyExistsException e) {
          throw e;
        } catch (IOException | UnsupportedOperationException e) {
          Files.copy(source, target);
        }
        break;
      case MV:
        Files.move(source, target);
        break;
      default:
        break;
    }
  }

  public static boolean isValidOperation(String operation) {
    return "none".equalsIgnoreCase(operation)
        || "mv".equalsIgnoreCase(operation)
        || "cp".equalsIgnoreCase(operation)
        || "delete".equalsIgnoreCase(operation);
  }

  public static ImportTsFileOperation getOperation(String operation, boolean isFileStoreEquals) {
    switch (operation.toLowerCase()) {
      case "none":
        return ImportTsFileOperation.NONE;
      case "mv":
        return ImportTsFileOperation.MV;
      case "cp":
        if (isFileStoreEquals) {
          return ImportTsFileOperation.HARDLINK;
        } else {
          return ImportTsFileOperation.CP;
        }
      case "delete":
        return ImportTsFileOperation.DELETE;
      default:
        new IoTPrinter(System.out).println("Args error: os/of must be one of none, mv, cp, delete");
        System.exit(Constants.CODE_ERROR);
        return null;
    }
  }
}
