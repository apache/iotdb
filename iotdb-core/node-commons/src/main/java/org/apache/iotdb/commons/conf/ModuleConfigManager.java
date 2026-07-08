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

package org.apache.iotdb.commons.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Module configuration manager that reads build-time injected module switches. These configurations
 * are set during Maven build process using resource filtering.
 */
public class ModuleConfigManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ModuleConfigManager.class);
  private static final String CONFIG_FILE = "module-config.properties";
  private static final ModuleConfigManager INSTANCE = new ModuleConfigManager();

  private final Properties properties;
  private final boolean moduleAEnabled;
  private final boolean moduleBEnabled;
  private final String versionSuffix;
  private final Edition edition;

  private ModuleConfigManager() {
    properties = new Properties();
    loadProperties();

    // Parse module configurations
    moduleAEnabled = Boolean.parseBoolean(properties.getProperty("moduleA.enabled", "true"));
    moduleBEnabled = Boolean.parseBoolean(properties.getProperty("moduleB.enabled", "true"));
    versionSuffix = properties.getProperty("version.suffix", "");
    edition = Edition.fromString(properties.getProperty("edition", "MAX"));
  }

  public static ModuleConfigManager getInstance() {
    return INSTANCE;
  }

  private void loadProperties() {
    try (InputStream input = getClass().getClassLoader().getResourceAsStream(CONFIG_FILE)) {
      if (input != null) {
        properties.load(input);
      }
    } catch (IOException e) {
      LOGGER.error("The installation package may be corrupted. Please reinstall the application.");
      System.exit(-1);
    }
  }

  /**
   * Check if Module A is enabled.
   *
   * @return true if Module A is enabled
   */
  public boolean isModuleAEnabled() {
    return moduleAEnabled;
  }

  /**
   * Check if Module B is enabled.
   *
   * @return true if Module B is enabled
   */
  public boolean isModuleBEnabled() {
    return moduleBEnabled;
  }

  /**
   * Get the version suffix for this build.
   *
   * @return version suffix (e.g., "-basic" or empty string)
   */
  public String getVersionSuffix() {
    return versionSuffix;
  }

  /**
   * @return the build-time edition (MAX by default).
   */
  public Edition getEdition() {
    return edition;
  }

  /**
   * @return true if this is a PRO edition build.
   */
  public boolean isPro() {
    return edition == Edition.PRO;
  }

  /**
   * @return true if this is a MAX edition build.
   */
  public boolean isMax() {
    return edition == Edition.MAX;
  }

  /**
   * Get a specific property value.
   *
   * @param key property key
   * @param defaultValue default value if not found
   * @return property value or default value
   */
  public String getProperty(String key, String defaultValue) {
    return properties.getProperty(key, defaultValue);
  }
}
