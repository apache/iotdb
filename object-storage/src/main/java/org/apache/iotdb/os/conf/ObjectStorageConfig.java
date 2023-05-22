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
package org.apache.iotdb.os.conf;

import org.apache.iotdb.os.io.aws.AWSS3Config;
import org.apache.iotdb.os.utils.ObjectStorageType;

import java.io.File;

public class ObjectStorageConfig {
  private ObjectStorageType osType = ObjectStorageType.AWS_S3;

  private AWSS3Config awss3Config = new AWSS3Config();

  private String[] cacheDirs = {
    "data" + File.separator + "datanode" + File.separator + "data" + File.separator + "cache"
  };

  private long cacheMaxDiskUsage = 20 * 1024 * 1024 * 1024L;

  private int cachePageSize = 10 * 1024 * 1024;

  ObjectStorageConfig() {}

  public ObjectStorageType getOsType() {
    return osType;
  }

  public void setOsType(ObjectStorageType osType) {
    this.osType = osType;
  }

  public String[] getCacheDirs() {
    return cacheDirs;
  }

  public String getBucketName() {
    return AWSS3Config.getBucketName();
  }

  public void setCacheDirs(String[] cacheDirs) {
    this.cacheDirs = cacheDirs;
  }

  public long getCacheMaxDiskUsage() {
    return cacheMaxDiskUsage;
  }

  public int getCachePageSize() {
    return cachePageSize;
  }

  public void setCachePageSize(int cachePageSize) {
    this.cachePageSize = cachePageSize;
  }
}
