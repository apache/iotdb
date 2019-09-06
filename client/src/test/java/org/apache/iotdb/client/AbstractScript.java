/**
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
package org.apache.iotdb.client;

import static org.junit.Assert.assertEquals;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractScript {

  protected void testOutput(ProcessBuilder builder, String[] output) throws IOException {
    builder.redirectErrorStream(true);
    Process p = builder.start();
    BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
    String line;
    List<String> outputList = new ArrayList<>();
    while (true) {
      line = r.readLine();
      if (line == null) {
        break;
      } else {
        outputList.add(line);
      }
    }
    r.close();
    p.destroy();

    for (int i = 0; i < output.length; i++) {
      assertEquals(output[output.length - 1 - i], outputList.get(outputList.size() - 1 - i));
    }
  }

  protected String getCliPath() {
    // This is usually always set by the JVM
    File userDir = new File(System.getProperty("user.dir"));
    if(!userDir.exists()) {
      throw new RuntimeException("user.dir " + userDir.getAbsolutePath() + " doesn't exist.");
    }
    File targetDir = new File(userDir, "target");
    File[] files = targetDir.listFiles(pathname -> pathname.isDirectory() && pathname.getName().startsWith("iotdb-client-"));
    if(files.length != 1) {
      throw new RuntimeException(
              "Exactly one directory starting with 'iotdb-client-' should have been found, but was " + files.length);
    }
    return files[0].getAbsolutePath();
  }

  protected abstract void testOnWindows() throws IOException;

  protected abstract void testOnUnix() throws IOException;
}
