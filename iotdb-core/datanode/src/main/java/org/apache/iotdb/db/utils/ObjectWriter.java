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

package org.apache.iotdb.db.utils;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ObjectWriter implements AutoCloseable {

  private final FileOutputStream fos;

  public ObjectWriter(String filePath) throws FileNotFoundException {
    // TODO:[OBJECT] Dir creation
    Path path = Paths.get(filePath);
    if (!Files.exists(path)) {
      try {
        Files.createFile(path);
      } catch (IOException e) {
        e.printStackTrace();
        throw new FileNotFoundException(e.getMessage());
      }
    }
    fos = new FileOutputStream(filePath, true);
  }

  public void write(byte[] content) throws IOException {
    fos.write(content);
  }

  @Override
  public void close() throws Exception {
    fos.close();
  }
}
