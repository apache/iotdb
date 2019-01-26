/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.filenode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is used to serialize or deserialize the object T.
 */
public class SerializeUtil<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SerializeUtil.class);

  /**
   * serialize obj and write to filePath.
   */
  public void serialize(Object obj, String filePath) throws IOException {
    try (FileOutputStream fileOutputStream = new FileOutputStream(filePath);
         ObjectOutputStream oos = new ObjectOutputStream(fileOutputStream)) {
      oos.writeObject(obj);
      oos.flush();
    } catch (IOException e) {
      LOGGER.error("Serizelize the object failed.", e);
      throw e;
    }
  }

  /**
   * deserialize obj from filePath.
   */
  public Optional<T> deserialize(String filePath) throws IOException {
    File file = new File(filePath);
    if (!file.exists()) {
      return Optional.empty();
    }
    T result;
    try (FileInputStream fis = new FileInputStream(file);
         ObjectInputStream ois = new ObjectInputStream(fis)) {
      result = (T) ois.readObject();
    } catch (Exception e) {
      LOGGER.error("Deserialize the object error.", e);
      return Optional.empty();
    }
    return Optional.ofNullable(result);
  }

}
