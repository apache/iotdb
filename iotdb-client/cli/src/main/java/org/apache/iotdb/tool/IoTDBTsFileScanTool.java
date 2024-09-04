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

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

public class IoTDBTsFileScanTool {
  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);

  private static final String RESOURCE = ".resource";
  private static final String MODS = ".mods";

  private static final LinkedBlockingQueue<String> tsfileQueue = new LinkedBlockingQueue<>();
  private static final Set<String> tsfileSet = new HashSet<>();
  private static final Set<String> resourceOrModsSet = new HashSet<>();
  private static String sourceFullPath;

  public static void traverseAndCollectFiles() throws InterruptedException {
    traverseAndCollectFilesBySourceFullPath(new File(sourceFullPath));
  }

  private static void traverseAndCollectFilesBySourceFullPath(final File file)
      throws InterruptedException {
    if (file.isFile()) {
      if (file.getName().endsWith(RESOURCE) || file.getName().endsWith(MODS)) {
        resourceOrModsSet.add(file.getAbsolutePath());
      } else {
        tsfileSet.add(file.getAbsolutePath());
        tsfileQueue.put(file.getAbsolutePath());
      }
    } else if (file.isDirectory()) {
      final File[] files = file.listFiles();
      if (files != null) {
        for (File f : files) {
          traverseAndCollectFilesBySourceFullPath(f);
        }
      }
    }
  }

  public static void addNoResourceOrModsToQueue() throws InterruptedException {
    for (final String filePath : resourceOrModsSet) {
      final String tsfilePath =
          filePath.endsWith(RESOURCE)
              ? filePath.substring(0, filePath.length() - RESOURCE.length())
              : filePath.substring(0, filePath.length() - MODS.length());
      if (!tsfileSet.contains(tsfilePath)) {
        tsfileQueue.put(filePath);
      }
    }
  }

  public static boolean isContainModsFile(final String filePath) {
    return IoTDBTsFileScanTool.resourceOrModsSet.contains(filePath);
  }

  public static String getFilePath() {
    return IoTDBTsFileScanTool.tsfileQueue.poll();
  }

  public static void put(final String filePath) {
    try {
      IoTDBTsFileScanTool.tsfileQueue.put(filePath);
    } catch (final InterruptedException e) {
      ioTPrinter.println("add file error");
    }
  }

  public static void setSourceFullPath(final String sourceFullPath) {
    IoTDBTsFileScanTool.sourceFullPath = sourceFullPath;
  }

  public static int getSourceFullPathLength() {
    return IoTDBTsFileScanTool.sourceFullPath.length();
  }

  public static int getTsFileQueueSize() {
    return IoTDBTsFileScanTool.tsfileQueue.size();
  }
}
