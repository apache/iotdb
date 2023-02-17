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

package org.apache.iotdb.tsfile.common.conf;

import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.utils.Loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

/** TSFileDescriptor is used to load TSFileConfig and provide configure information. */
public class TSFileDescriptor {

  private static final Logger logger = LoggerFactory.getLogger(TSFileDescriptor.class);
  private final TSFileConfig conf = new TSFileConfig();

  /** The constructor just visible for test */
  /* private */ TSFileDescriptor() {
    init();
  }

  public static TSFileDescriptor getInstance() {
    return TsfileDescriptorHolder.INSTANCE;
  }

  public TSFileConfig getConfig() {
    return conf;
  }

  private void init() {
    Properties properties = loadProperties();
    if (properties != null) {
      overwriteConfigByCustomSettings(properties);
    }
  }

  public void overwriteConfigByCustomSettings(Properties properties) {
    PropertiesOverWriter writer = new PropertiesOverWriter(properties);

    writer.setInt(conf::setGroupSizeInByte, "group_size_in_byte");
    writer.setInt(conf::setPageSizeInByte, "page_size_in_byte");
    if (conf.getPageSizeInByte() > conf.getGroupSizeInByte()) {
      int groupSizeInByte = conf.getGroupSizeInByte();
      logger.warn(
          "page_size is greater than group size, will set it as the same with group size {}",
          groupSizeInByte);
      conf.setPageSizeInByte(groupSizeInByte);
    }
    writer.setInt(conf::setMaxNumberOfPointsInPage, "max_number_of_points_in_page");
    writer.setInt(conf::setMaxDegreeOfIndexNode, "max_degree_of_index_node");
    writer.setInt(conf::setMaxStringLength, "max_string_length");
    writer.setInt(conf::setFloatPrecision, "float_precision");
    writer.setString(conf::setTimeEncoder, "time_encoder");
    writer.setString(conf::setValueEncoder, "value_encoder");
    writer.setString(conf::setCompressor, "compressor");
    writer.setInt(conf::setBatchSize, "batch_size");
    writer.setInt(conf::setFreqEncodingBlockSize, "freq_block_size");
    writer.setDouble(conf::setFreqEncodingSNR, "freq_snr");
  }

  private class PropertiesOverWriter {

    private final Properties properties;

    public PropertiesOverWriter(Properties properties) {
      if (properties == null) {
        throw new NullPointerException("properties should not be null");
      }
      this.properties = properties;
    }

    public void setInt(Consumer<Integer> setter, String propertyKey) {
      set(setter, propertyKey, Integer::parseInt);
    }

    public void setDouble(Consumer<Double> setter, String propertyKey) {
      set(setter, propertyKey, Double::parseDouble);
    }

    public void setString(Consumer<String> setter, String propertyKey) {
      set(setter, propertyKey, Function.identity());
    }

    private <T> void set(
        Consumer<T> setter, String propertyKey, Function<String, T> propertyValueConverter) {
      String value = this.properties.getProperty(propertyKey);

      if (value != null) {
        try {
          T v = propertyValueConverter.apply(value);
          setter.accept(v);
        } catch (Exception e) {
          logger.warn("invalid value for {}, use the default value", propertyKey);
        }
      }
    }
  }

  private Properties loadProperties() {
    String file = detectPropertiesFile();
    if (file != null) {
      logger.info("try loading {} from {}", TSFileConfig.CONFIG_FILE_NAME, file);
      return loadPropertiesFromFile(file);
    } else {
      logger.warn("not found {}, use the default configs.", TSFileConfig.CONFIG_FILE_NAME);
      return null;
    }
  }

  private Properties loadPropertiesFromFile(String filePath) {
    try (FileInputStream fileInputStream = new FileInputStream(filePath)) {
      Properties properties = new Properties();
      properties.load(fileInputStream);
      return properties;
    } catch (FileNotFoundException e) {
      logger.warn("Fail to find config file {}", filePath);
      return null;
    } catch (IOException e) {
      logger.warn("read file ({}) failure, please check the access permissions.", filePath);
      return null;
    }
  }

  private String detectPropertiesFile() {
    String confDirectory = System.getProperty(TsFileConstant.TSFILE_CONF);
    if (confDirectory != null) {
      return Paths.get(confDirectory, TSFileConfig.CONFIG_FILE_NAME).toAbsolutePath().toString();
    }
    String tsFileHome = System.getProperty(TsFileConstant.TSFILE_HOME);
    if (tsFileHome != null) {
      return Paths.get(tsFileHome, "conf", TSFileConfig.CONFIG_FILE_NAME)
          .toAbsolutePath()
          .toString();
    }

    return detectPropertiesFromClassPath();
  }

  private static URL getResource(String filename, ClassLoader classLoader) {
    return Loader.getResource(filename, classLoader);
  }

  private String detectPropertiesFromClassPath() {
    ClassLoader classLoader = Loader.getClassLoaderOfObject(this);
    URL u = getResource(TSFileConfig.CONFIG_FILE_NAME, classLoader);
    if (u == null) {
      return null;
    } else {
      multiplicityWarning(TSFileConfig.CONFIG_FILE_NAME, classLoader);
      return u.getFile();
    }
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

  private static class TsfileDescriptorHolder {

    private TsfileDescriptorHolder() {
      throw new IllegalAccessError("Utility class");
    }

    private static final TSFileDescriptor INSTANCE = new TSFileDescriptor();
  }
}
