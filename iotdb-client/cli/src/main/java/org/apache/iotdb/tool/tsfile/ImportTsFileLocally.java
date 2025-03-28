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

package org.apache.iotdb.tool.tsfile;

import org.apache.iotdb.cli.utils.IoTPrinter;
import org.apache.iotdb.session.pool.SessionPool;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator.DATABASE_NAME_KEY;
import static org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator.VERIFY_KEY;

public class ImportTsFileLocally extends ImportTsFileBase implements Runnable {

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);

  private static SessionPool sessionPool;
  private static boolean verify;

  @Override
  public void loadTsFile() {
    String filePath;
    try {
      while ((filePath = ImportTsFileScanTool.pollFromQueue()) != null) {
        String sql = buildLoadSql(filePath);
        try {
          sessionPool.executeNonQueryStatement(sql);
          processSuccessFile(filePath);
        } catch (final Exception e) {
          processFailFile(filePath, e);
        }
      }
    } catch (final Exception e) {
      ioTPrinter.println("Unexpected error occurred: " + e.getMessage());
    }
  }

  private String buildLoadSql(String filePath) {
    List<String> parameters = new ArrayList<>();
    if (!verify) {
      parameters.add(String.format("\"%s\"=false", VERIFY_KEY));
    }
    if (databaseName != null) {
      parameters.add(String.format("\"%s\"=\"%s\"", DATABASE_NAME_KEY, databaseName));
    }
    String paramStr = String.join(",", parameters);
    if (paramStr.isEmpty()) {
      return String.format("load '%s'", filePath);
    } else {
      return String.format("load '%s' WITH (%s)", filePath, paramStr);
    }
  }

  public static void setSessionPool(SessionPool sessionPool) {
    ImportTsFileLocally.sessionPool = sessionPool;
  }

  public static void setVerify(boolean verify) {
    ImportTsFileLocally.verify = verify;
  }
}
