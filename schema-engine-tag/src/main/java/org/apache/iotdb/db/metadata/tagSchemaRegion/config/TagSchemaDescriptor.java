/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.metadata.tagSchemaRegion.config;

import org.apache.iotdb.commons.conf.IoTDBConstant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/** manager tag schema config */
public class TagSchemaDescriptor {
  private static final Logger logger = LoggerFactory.getLogger(TagSchemaDescriptor.class);

  private static final String TAG_SCHEMA_CONFIG_FILE_NAME = "schema-tag.properties";

  private final TagSchemaConfig conf = new TagSchemaConfig();

  private TagSchemaDescriptor() {
    loadProperties();
  }

  public static TagSchemaDescriptor getInstance() {
    return TagSchemaDescriptorHolder.INSTANCE;
  }

  private void loadProperties() {
    String iotDBHomePath = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
    if (iotDBHomePath == null) {
      logger.warn(
          "Cannot find IOTDB_HOME environment variable when loading "
              + "config file {}, use default configuration",
          TAG_SCHEMA_CONFIG_FILE_NAME);
      return;
    }
    String tagSchemaConfigPath =
        iotDBHomePath
            + File.separatorChar
            + "conf"
            + File.separatorChar
            + TAG_SCHEMA_CONFIG_FILE_NAME;
    try (InputStream in = new BufferedInputStream(new FileInputStream(tagSchemaConfigPath))) {
      Properties properties = new Properties();
      properties.load(in);
      conf.setWalBufferSize(
          Integer.parseInt(
              properties.getProperty("wal_buffer_size", String.valueOf(conf.getWalBufferSize()))));
      conf.setNumOfDeviceIdsInMemTable(
          Integer.parseInt(
              properties.getProperty(
                  "num_of_deviceIds_in_memTable",
                  String.valueOf(conf.getNumOfDeviceIdsInMemTable()))));
    } catch (FileNotFoundException e) {
      logger.warn("Fail to find tag schema region config file {}", tagSchemaConfigPath);
    } catch (IOException e) {
      logger.warn("Cannot load tag schema region config file, use default configuration");
    }
  }

  public TagSchemaConfig getTagSchemaConfig() {
    return conf;
  }

  private static class TagSchemaDescriptorHolder {

    private static final TagSchemaDescriptor INSTANCE = new TagSchemaDescriptor();

    private TagSchemaDescriptorHolder() {}
  }
}
