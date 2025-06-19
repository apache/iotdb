/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.wal.utils;

import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALReader;

import java.io.File;
import java.io.IOException;
import java.util.Stack;

public class WALPrintTool {

  public void print(File file) throws IOException {
    Stack<File> stack = new Stack<>();
    if (file.exists()) {
      stack.push(file);
    } else {
      System.out.println("The initial file does not exist");
    }

    while (!stack.isEmpty()) {
      File f = stack.pop();
      if (f.isDirectory()) {
        File[] files = f.listFiles();
        if (files != null) {
          for (File child : files) {
            stack.push(child);
          }
        }
      } else if (f.getName().endsWith(".wal")) {
        doPrint(f);
      }
    }
  }

  private void doPrint(File file) throws IOException {
    System.out.printf("-----------------%s---------------%n", file.getAbsoluteFile());
    try (WALReader reader = new WALReader(file)) {
      long walCurrentReadOffset = reader.getWALCurrentReadOffset();
      while (reader.hasNext()) {
        WALEntry entry = reader.next();
        System.out.printf("%d\t%s%n", walCurrentReadOffset, entry.toString());
        walCurrentReadOffset = reader.getWALCurrentReadOffset();
      }
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      System.out.println("Usage: WALPrintTool <file>");
      return;
    }

    WALPrintTool tool = new WALPrintTool();
    tool.print(new File(args[0]));
  }
}
