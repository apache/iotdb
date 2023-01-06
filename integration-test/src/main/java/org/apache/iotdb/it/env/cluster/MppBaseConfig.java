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
package org.apache.iotdb.it.env.cluster;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/** this class stands for a config object of one single property file */
public abstract class MppBaseConfig {

  public static final String NULL_VALUE = "";
  private final Properties properties;

  /** Create an empty MppPersistentConfig. */
  public MppBaseConfig() {
    this.properties = new Properties();
  }

  /**
   * Create a MppPersistentConfig from the property file.
   *
   * @param filePath the property file path.
   */
  public MppBaseConfig(String filePath) throws IOException {
    this();
    updateProperties(filePath);
  }

  /**
   * Put all the key-value pairs of input properties to the current instance one. If the key is
   * existed in both, the value will be overridden.
   *
   * @param properties the properties to update with.
   */
  public final void updateProperties(@NotNull Properties properties) {
    properties.forEach(
        (k, v) -> {
          this.setProperty((String) k, (String) v);
        });
  }

  /**
   * Put all the properties of the input persistent config to the current instance. If the key is
   * existed in both, the value will be overridden.
   *
   * @param persistentConfig the config instance to update with.
   */
  public void updateProperties(@NotNull MppBaseConfig persistentConfig) {
    updateProperties(persistentConfig.properties);
  }

  /**
   * Put all the properties of the input file to the current instance. If the key is existed in
   * both, the value will be overridden.
   *
   * @param filePath the path of the properties file to update with. If it's null, then nothing will
   *     happen.
   * @throws IOException there's some errors during processing the file.
   */
  public final void updateProperties(@Nullable String filePath) throws IOException {
    if (filePath == null) {
      return;
    }
    try (InputStream confInput = Files.newInputStream(Paths.get(filePath))) {
      properties.load(confInput);
    }
  }

  /**
   * Persistent the properties to the file.
   *
   * @param filePath the file path.
   */
  public final void persistent(String filePath) throws IOException {
    try (FileWriter confOutput = new FileWriter(filePath)) {
      properties.store(confOutput, null);
    }
  }

  /**
   * Set a property with the input key and value. If the key is existed, the value will be
   * overridden. If the value is an empty String, the key will be removed.
   *
   * @param key the key.
   * @param value the value.
   */
  protected final void setProperty(@NotNull String key, String value) {
    if (!value.equals(NULL_VALUE)) {
      properties.setProperty(key, value);
    } else {
      properties.remove(key);
    }
  }

  /** Create an instance but with empty properties. */
  public abstract MppBaseConfig emptyClone();
}
