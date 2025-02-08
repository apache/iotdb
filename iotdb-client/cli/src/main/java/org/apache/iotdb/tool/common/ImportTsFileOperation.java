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

public enum ImportTsFileOperation {
  NONE,
  MV,
  HARDLINK,
  CP,
  DELETE,
  ;

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
