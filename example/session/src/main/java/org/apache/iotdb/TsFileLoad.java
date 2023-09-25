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

package org.apache.iotdb;

import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

public class TsFileLoad {

  private static final String RENAME_SUFFIX = ".rename";

  // parallelism ip port username password tsfileDirs...
  public static void main(String[] args) {
    long startTime = System.nanoTime();

    final int parallelism = Integer.parseInt(args[0]);
    final String ip = args[1];
    final int port = Integer.parseInt(args[2]);
    final String username = args[3];
    final String password = args[4];
    File[] tsfileDirs = new File[args.length - 5];
    for (int i = 5, n = args.length; i < n; i++) {
      tsfileDirs[i - 5] = new File(args[i]);
    }
    List<String> tsfileList = new ArrayList<>();
    for (File tsfileDir : tsfileDirs) {
      if (tsfileDir.exists()) {
        if (tsfileDir.isDirectory()) {
          try (Stream<Path> paths = Files.walk(tsfileDir.toPath())) {
            paths
                .map(Path::toString)
                .filter(f -> f.endsWith(RENAME_SUFFIX))
                .forEach(tsfileList::add);
          } catch (IOException e) {
            System.out.println("Collect tsfile failed!");
            e.printStackTrace();
          }
        } else if (tsfileDir.isFile() && tsfileDir.getName().endsWith(RENAME_SUFFIX)) {
          tsfileList.add(tsfileDir.getName());
        } else {
          System.out.println("skip invalid tsfileDir: " + tsfileDir);
        }
      }
    }

    for (String tsfileName : tsfileList) {
      File oldFile = new File(tsfileName);
      if (!oldFile.renameTo(new File(tsfileName.substring(0, tsfileName.length() - 7)))) {
        System.out.println("Failed to remove .rename suffix: " + tsfileName);
      }
    }

    tsfileList.clear();
    for (File tsfileDir : tsfileDirs) {
      if (tsfileDir.exists()) {
        if (tsfileDir.isDirectory()) {
          try (Stream<Path> paths = Files.walk(tsfileDir.toPath())) {
            paths
                .map(Path::toString)
                .filter(f -> f.endsWith(TSFILE_SUFFIX))
                .forEach(tsfileList::add);
          } catch (IOException e) {
            System.out.println("Collect tsfile failed!");
            e.printStackTrace();
          }
        } else if (tsfileDir.isFile() && tsfileDir.getName().endsWith(TSFILE_SUFFIX)) {
          tsfileList.add(tsfileDir.getName());
        } else {
          System.out.println("skip invalid tsfileDir: " + tsfileDir);
        }
      }
    }

    System.out.println(
        "Total renamed tsfile number: " + tsfileList.size() + System.lineSeparator());

    CountDownLatch countDownLatch = new CountDownLatch(parallelism);
    List<String> failedTsFileList = new ArrayList<>();
    AtomicInteger index = new AtomicInteger(0);
    AtomicInteger completed = new AtomicInteger(0);
    AtomicInteger failed = new AtomicInteger(0);

    for (int i = 0; i < parallelism; i++) {
      new Thread(
              () -> {
                Session session =
                    new Session.Builder()
                        .host(ip)
                        .port(port)
                        .username(username)
                        .password(password)
                        .version(Version.V_1_0)
                        .build();
                try {
                  session.open(false);
                } catch (IoTDBConnectionException e) {
                  countDownLatch.countDown();
                  System.out.println(
                      "failed to build session, ip: "
                          + ip
                          + ", port: "
                          + port
                          + ", username: "
                          + username
                          + ", password: "
                          + password);
                  e.printStackTrace();
                  return;
                }
                int fileIndex = index.getAndIncrement();
                while (fileIndex < tsfileList.size()) {
                  String tsfilePath = tsfileList.get(fileIndex);
                  String loadTsFileSql = "load '" + tsfilePath + "' sgLevel=1";
                  try {
                    System.out.println("start to execute: " + loadTsFileSql);
                    session.executeNonQueryStatement(loadTsFileSql);
                    System.out.println("succeed to execute: " + loadTsFileSql);
                    completed.incrementAndGet();
                  } catch (IoTDBConnectionException | StatementExecutionException e) {
                    e.printStackTrace();
                    System.out.println("failed to execute: " + loadTsFileSql);
                    failedTsFileList.add(tsfilePath);
                    failed.incrementAndGet();
                  }
                  System.out.println(
                      "rename completed: "
                          + completed.get()
                          + ", failed: "
                          + failed.get()
                          + ", total: "
                          + tsfileList.size()
                          + ", progress: "
                          + (((double) completed.get() + failed.get()) / tsfileList.size())
                          + ", elapse time: "
                          + (System.nanoTime() - startTime) / 1_000_000_000
                          + "s");
                  fileIndex = index.getAndIncrement();
                }
                try {
                  session.close();
                } catch (IoTDBConnectionException e) {
                  System.out.println("failed to close session.");
                  e.printStackTrace();
                }
                countDownLatch.countDown();
              })
          .start();
    }

    while (true) {
      try {
        countDownLatch.await();
        if (!failedTsFileList.isEmpty()) {
          System.out.println("Failed tsfile List: ");
          failedTsFileList.forEach(System.out::println);
        }
        break;
      } catch (InterruptedException e) {
        // ignore
      }
    }
  }
}
