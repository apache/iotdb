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

package org.apache.iotdb.tool.data;

import java.io.File;
import java.util.concurrent.LinkedBlockingQueue;

public class ImportDataScanTool {

  private static final LinkedBlockingQueue<String> dataQueue = new LinkedBlockingQueue<>();
  private static String sourceFullPath;

  public static void traverseAndCollectFiles() throws InterruptedException {
    traverseAndCollectFilesBySourceFullPath(new File(sourceFullPath));
  }

  private static void traverseAndCollectFilesBySourceFullPath(final File file)
      throws InterruptedException {
    if (file.isFile()) {
      putToQueue(file.getAbsolutePath());
    } else if (file.isDirectory()) {
      final File[] files = file.listFiles();
      if (files != null) {
        for (File f : files) {
          traverseAndCollectFilesBySourceFullPath(f);
        }
      }
    }
  }

  public static String pollFromQueue() {
    return ImportDataScanTool.dataQueue.poll();
  }

  public static void putToQueue(final String filePath) throws InterruptedException {
    ImportDataScanTool.dataQueue.put(filePath);
  }

  public static void setSourceFullPath(final String sourceFullPath) {
    ImportDataScanTool.sourceFullPath = sourceFullPath;
  }

  public static int getTsFileQueueSize() {
    return ImportDataScanTool.dataQueue.size();
  }
}
