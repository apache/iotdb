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

package org.apache.iotdb.tool;

import org.apache.iotdb.cli.utils.IoTPrinter;
import org.apache.iotdb.session.pool.SessionPool;

public class ImportTsFileLocally extends AbstractTsFileProcessTool implements Runnable {

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);

  private static SessionPool sessionPool;

  @Override
  public void run() {
    loadTsFile();
  }

  @Override
  public void loadTsFile() {
    String filePath;
    try {
      while ((filePath = IoTDBTsFileScanAndProcessTool.getFilePath()) != null) {
        final String sql = "load '" + filePath + "' onSuccess=none ";
        try {
          sessionPool.executeNonQueryStatement(sql);

          processSuccessFile(filePath);
        } catch (final Exception e) {
          processFailFile(filePath, e);
        }
      }
    } catch (Exception e) {
      ioTPrinter.println("Unexpected error occurred: " + e.getMessage());
    }
  }

  public static void setSessionPool(SessionPool sessionPool) {
    ImportTsFileLocally.sessionPool = sessionPool;
  }
}
