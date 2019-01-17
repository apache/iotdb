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
 *
 * @author kangrong
 * @author liukun
 */
public class SerializeUtil<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SerializeUtil.class);

  /**
   * serialize obj and write to filePath.
   */
  public void serialize(Object obj, String filePath) throws IOException {
    ObjectOutputStream oos = null;
    try {
      oos = new ObjectOutputStream(new FileOutputStream(filePath));
      oos.writeObject(obj);
      oos.flush();
    } catch (IOException e) {
      LOGGER.error("Serizelize the object failed.", e);
      throw e;
    } finally {
      if (oos != null) {
        oos.close();
      }
    }
  }

  /**
   * deserialize obj from filePath.
   */
  public Optional<T> deserialize(String filePath) throws IOException {
    ObjectInputStream ois = null;
    File file = new File(filePath);
    if (!file.exists()) {
      return Optional.empty();
    }
    T result = null;
    try {
      ois = new ObjectInputStream(new FileInputStream(file));
      result = (T) ois.readObject();
      ois.close();
    } catch (Exception e) {
      LOGGER.error("Deserialize the object error.", e);
      if (ois != null) {
        ois.close();
      }
      return Optional.empty();
    }
    return Optional.ofNullable(result);
  }

}
