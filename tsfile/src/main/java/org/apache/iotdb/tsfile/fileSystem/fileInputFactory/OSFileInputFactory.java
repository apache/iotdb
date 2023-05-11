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
package org.apache.iotdb.tsfile.fileSystem.fileInputFactory;

import org.apache.iotdb.tsfile.read.reader.TsFileInput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class OSFileInputFactory implements FileInputFactory {
  private static final Logger logger = LoggerFactory.getLogger(OSFileInputFactory.class);
  private static final String OS_INPUT_CLASS_NAME = "org.apache.iotdb.os.fileSystem.OSInput";
  private static Constructor constructor;

  static {
    try {
      Class<?> clazz = Class.forName(OS_INPUT_CLASS_NAME);
      constructor = clazz.getConstructor(String.class);
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      logger.error(
          "Failed to get OSInput in object storage. Please check your dependency of object storage module.",
          e);
    }
  }

  @Override
  public TsFileInput getTsFileInput(String filePath) throws IOException {
    try {
      return (TsFileInput) constructor.newInstance(filePath);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      throw new IOException(
          String.format(
              "Failed to get TsFile input of file: %s. Please check your dependency of object storage module.",
              filePath),
          e);
    }
  }
}
