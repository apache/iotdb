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
package org.apache.iotdb.db.exception.builder;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExceptionBuilder {

  public static final int UNKNOWN_ERROR = 20000;
  public static final int NO_PARAMETERS_EXISTS = 20001;
  public static final int INVALID﻿_PARAMETER_NO = 20002;
  public static final int CONN_HOST_ERROR = 20003;
  public static final int AUTH_PLUGIN_ERR = 20061;
  public static final int INSECURE_API_ERR = 20062;
  public static final int OUT_OF_MEMORY = 20064;
  public static final int NO_PREPARE_STMT = 20130;
  public static final int CON_FAIL_ERR = 20220;
  public static final String CONFIG_NAME = "error_info_";
  public static final String FILE_SUFFIX = ".properties";
  public static final String DEFAULT_FILEPATH = "error_info_en.properties";
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDescriptor.class);
  private static final ExceptionBuilder INSTANCE = new ExceptionBuilder();
  private Properties properties = new Properties();

  public static final ExceptionBuilder getInstance() {
    return ExceptionBuilder.INSTANCE;
  }

  /**
   * load error information file.
   *
   * @param filePath the path of error information file
   */
  public void loadInfo(String filePath) {
    InputStream in = null;
    try {
      in = new BufferedInputStream(new FileInputStream(filePath));
      properties.load(new InputStreamReader(in, "utf-8"));
      in.close();
    } catch (IOException e) {
      LOGGER.error(
          "Read file error. File does not exist or file is broken. "
              + "File seriesPath: {}.Because: {}.",
          filePath, e.getMessage());
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          LOGGER.error("Fail to close file: {}. Because: {}.", filePath, e.getMessage());
        }
      }
    }
  }

  /**
   * load information.
   */
  public void loadInfo() {
    String language = IoTDBDescriptor.getInstance().getConfig().languageVersion.toLowerCase();

    String url = System.getProperty(IoTDBConstant.IOTDB_CONF, null);
    if (url == null) {
      url = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
      if (url != null) {
        url = url + File.separatorChar + "conf" + File.separatorChar + ExceptionBuilder.CONFIG_NAME
            + language
            + FILE_SUFFIX;
      } else {
        LOGGER.warn(
            "Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading config file {}"
                + ", use default configuration",
            IoTDBConfig.CONFIG_NAME);
        return;
      }
    } else {
      url += (File.separatorChar + ExceptionBuilder.CONFIG_NAME + language + FILE_SUFFIX);
    }

    File file = new File(url);
    if (!file.exists()) {
      url.replace(CONFIG_NAME + language + FILE_SUFFIX, DEFAULT_FILEPATH);
    }

    loadInfo(url);
  }

  public String searchInfo(int errCode) {
    return properties.getProperty(String.valueOf(errCode));
  }
}
