/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.query.externalsort.serialize.impl;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import org.apache.iotdb.db.query.externalsort.serialize.TimeValuePairSerializer;
import org.apache.iotdb.db.utils.TimeValuePair;

/**
 * Serializer timeValuePair.
 */
public class SimpleTimeValuePairSerializer implements TimeValuePairSerializer {

  private ObjectOutputStream objectOutputStream;

  /**
   * init with file path.
   */
  public SimpleTimeValuePairSerializer(String tmpFilePath) throws IOException {
    checkPath(tmpFilePath);
    objectOutputStream = new ObjectOutputStream(new BufferedOutputStream(
        new FileOutputStream(tmpFilePath)));
  }

  private void checkPath(String tmpFilePath) throws IOException {
    File file = new File(tmpFilePath);
    if (file.exists()) {
      file.delete();
    }
    if (file.getParentFile() != null) {
      file.getParentFile().mkdirs();
    }
    file.createNewFile();
  }

  @Override
  public void write(TimeValuePair timeValuePair) throws IOException {
    objectOutputStream.writeUnshared(timeValuePair);
  }

  @Override
  public void close() throws IOException {
    objectOutputStream.close();
  }
}
