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
package org.apache.iotdb.it.framework;

import org.apache.commons.io.FileUtils;
import org.junit.runner.Result;
import org.junit.runner.notification.RunListener;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@RunListener.ThreadSafe
public class IoTDBTestListener extends RunListener {

  public static final String statOutputDir = "target" + File.separator + "test-stats";
  public static final String statExt = ".stats";
  private final List<IoTDBTestStat> stats = new ArrayList<>();
  private final String statFileName;

  public IoTDBTestListener(String statFileName) {
    try {
      String dirName = System.getProperty("user.dir") + File.separator + statOutputDir;
      FileUtils.forceMkdir(new File(dirName));
      this.statFileName = dirName + File.separator + statFileName + statExt;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public synchronized void addTestStat(IoTDBTestStat stat) {
    stats.add(stat);
  }

  @Override
  public synchronized void testRunFinished(Result result) throws IOException {
    StringBuilder sb = new StringBuilder();
    synchronized (this) {
      for (IoTDBTestStat stat : stats) {
        sb.append(stat).append("\n");
      }
    }
    Files.write(Paths.get(statFileName), sb.toString().getBytes(StandardCharsets.UTF_8));
  }
}
