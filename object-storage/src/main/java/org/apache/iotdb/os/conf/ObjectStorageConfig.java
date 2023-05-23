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

import org.apache.iotdb.os.conf.provider.AWSS3Config;
import org.apache.iotdb.os.conf.provider.OSProviderConfig;
import org.apache.iotdb.os.conf.provider.TestConfig;
import org.apache.iotdb.os.utils.ObjectStorageType;

import java.io.File;

public class ObjectStorageConfig {
  private ObjectStorageType osType = ObjectStorageType.TEST;

  private OSProviderConfig providerConfig = new TestConfig();

  private String[] cacheDirs = {
    "data" + File.separator + "datanode" + File.separator + "data" + File.separator + "cache"
  };

  private long cacheMaxDiskUsage = 50 * 1024 * 1024L;

  private int cachePageSize = 10 * 1024 * 1024;

  ObjectStorageConfig() {}

  public ObjectStorageType getOsType() {
    return osType;
  }

  public void setOsType(ObjectStorageType osType) {
    this.osType = osType;
    switch (osType) {
      case AWS_S3:
        this.providerConfig = new AWSS3Config();
        break;
      default:
        this.providerConfig = new TestConfig();
    }
  }

  public OSProviderConfig getProviderConfig() {
    return providerConfig;
  }

  public String getEndpoint() {
    return providerConfig.getEndpoint();
  }

  public void setEndpoint(String endpoint) {
    providerConfig.setEndpoint(endpoint);
  }

  public String getBucketName() {
    return providerConfig.getBucketName();
  }

  public void setBucketName(String bucketName) {
    providerConfig.setBucketName(bucketName);
  }

  public String getAccessKeyId() {
    return providerConfig.getAccessKeyId();
  }

  public void setAccessKeyId(String accessKeyId) {
    providerConfig.setAccessKeyId(accessKeyId);
  }

  public String getAccessKeySecret() {
    return providerConfig.getAccessKeySecret();
  }

  public void setAccessKeySecret(String accessKeySecret) {
    providerConfig.setAccessKeySecret(accessKeySecret);
  }

  public String[] getCacheDirs() {
    return cacheDirs;
  }

  public void setCacheDirs(String[] cacheDirs) {
    this.cacheDirs = cacheDirs;
  }

  public long getCacheMaxDiskUsage() {
    return cacheMaxDiskUsage;
  }

  public void setCacheMaxDiskUsage(long cacheMaxDiskUsage) {
    this.cacheMaxDiskUsage = cacheMaxDiskUsage;
  }

  public int getCachePageSize() {
    return cachePageSize;
  }

  public void setCachePageSize(int cachePageSize) {
    this.cachePageSize = cachePageSize;
  }
}
