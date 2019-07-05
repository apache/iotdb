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

package org.apache.iotdb.tsfile.common.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import java.util.Set;
import org.apache.iotdb.tsfile.common.constant.SystemConstant;
import org.apache.iotdb.tsfile.utils.Loader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TSFileDescriptor is used to load TSFileConfig and provide configure information.
 *
 * @author kangrong
 */
public class TSFileDescriptor {

  private static final Logger logger = LoggerFactory.getLogger(TSFileDescriptor.class);
  private TSFileConfig conf = new TSFileConfig();

  private TSFileDescriptor() {
    loadProps();
  }

  public static final TSFileDescriptor getInstance() {
    return TsfileDescriptorHolder.INSTANCE;
  }

  public TSFileConfig getConfig() {
    return conf;
  }

  private void multiplicityWarning(String resource, ClassLoader classLoader) {
    try {
      Set<URL> urlSet = Loader.getResources(resource, classLoader);
      if (urlSet != null && urlSet.size() > 1) {
        logger.warn("Resource [{}] occurs multiple times on the classpath", resource);
        for (URL url : urlSet) {
          logger.warn("Resource [{}] occurs at [{}]", resource, url);
        }
      }
    } catch (IOException e) {
      logger.error("Failed to get url list for {}", resource);
    }
  }

  private static URL getResource(String filename, ClassLoader classLoader) {
    return Loader.getResource(filename, classLoader);
  }

  /**
   * load an .properties file and set TSFileConfig variables
   */
  private void loadProps() {
    InputStream inputStream;
    String url = System.getProperty(SystemConstant.TSFILE_CONF, null);
    if (url == null) {
      url = System.getProperty(SystemConstant.TSFILE_HOME, null);
      if (url != null) {
        url = url + File.separator + "conf" + File.separator + TSFileConfig.CONFIG_FILE_NAME;
      } else {
        ClassLoader classLoader = Loader.getClassLoaderOfObject(this);
        URL u = getResource(TSFileConfig.CONFIG_FILE_NAME, classLoader);
        if (u == null) {
          logger.warn("Failed to find config file {} at classpath, use default configuration",
              TSFileConfig.CONFIG_FILE_NAME);
          return;
        } else {
          multiplicityWarning(TSFileConfig.CONFIG_FILE_NAME, classLoader);
          url = u.getFile();
        }
      }
    }
    try {
      inputStream = new FileInputStream(new File(url));
    } catch (FileNotFoundException e) {
      logger.warn("Fail to find config file {}", url);
      return;
    }

    logger.info("Start to read config file {}", url);
    Properties properties = new Properties();
    try {
      properties.load(inputStream);
      TSFileConfig.groupSizeInByte = Integer
          .parseInt(
              properties.getProperty("group_size_in_byte",
                  Integer.toString(TSFileConfig.groupSizeInByte)));
      TSFileConfig.pageSizeInByte = Integer
          .parseInt(properties
              .getProperty("page_size_in_byte", Integer.toString(TSFileConfig.pageSizeInByte)));
      if (TSFileConfig.pageSizeInByte > TSFileConfig.groupSizeInByte) {
        logger.warn("page_size is greater than group size, will set it as the same with group size");
        TSFileConfig.pageSizeInByte = TSFileConfig.groupSizeInByte;
      }
      TSFileConfig.maxNumberOfPointsInPage = Integer.parseInt(
          properties
              .getProperty("max_number_of_points_in_page",
                  Integer.toString(TSFileConfig.maxNumberOfPointsInPage)));
      TSFileConfig.timeSeriesDataType = properties
          .getProperty("time_series_data_type", TSFileConfig.timeSeriesDataType);
      TSFileConfig.maxStringLength = Integer
          .parseInt(properties
              .getProperty("max_string_length", Integer.toString(TSFileConfig.maxStringLength)));
      TSFileConfig.floatPrecision = Integer
          .parseInt(properties
              .getProperty("float_precision", Integer.toString(TSFileConfig.floatPrecision)));
      TSFileConfig.timeEncoder = properties
          .getProperty("time_encoder", TSFileConfig.timeEncoder);
      TSFileConfig.valueEncoder = properties
          .getProperty("value_encoder", TSFileConfig.valueEncoder);
      TSFileConfig.compressor = properties.getProperty("compressor", TSFileConfig.compressor);
    } catch (IOException e) {
      logger.warn("Cannot load config file, use default configuration", e);
    } catch (Exception e) {
      logger.error("Loading settings {} failed", url, e);
    } finally {
      try {
        inputStream.close();
      } catch (IOException e) {
        logger.error("Failed to close stream for loading config", e);
      }

    }
  }

  private static class TsfileDescriptorHolder {

    private TsfileDescriptorHolder() {
      throw new IllegalAccessError("Utility class");
    }

    private static final TSFileDescriptor INSTANCE = new TSFileDescriptor();
  }
}
