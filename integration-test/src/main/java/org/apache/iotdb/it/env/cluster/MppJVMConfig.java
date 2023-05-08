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

import org.apache.iotdb.itbase.env.JVMConfig;

import javax.annotation.Nullable;

public class MppJVMConfig implements JVMConfig {

  private int initHeapSize;
  private int maxHeapSize;
  private int maxDirectMemorySize;

  @Override
  public JVMConfig setInitHeapSize(int initSize) {
    if (initSize > 0) {
      this.initHeapSize = initSize;
    }
    return this;
  }

  @Override
  public JVMConfig setMaxHeapSize(int maxSize) {
    if (maxSize > 0) {
      this.maxHeapSize = maxSize;
    }
    return this;
  }

  public JVMConfig setMaxDirectMemorySize(int maxSize) {
    if (maxSize > 0) {
      this.maxDirectMemorySize = maxSize;
    }
    return this;
  }

  public int getInitHeapSize() {
    return initHeapSize;
  }

  public int getMaxHeapSize() {
    return maxHeapSize;
  }

  public int getMaxDirectMemorySize() {
    return maxDirectMemorySize;
  }

  private void validate() {
    if (initHeapSize > maxHeapSize) {
      throw new IllegalArgumentException(
          "the initHeapSize "
              + initHeapSize
              + " must be not larger than the maxHeapSize "
              + maxHeapSize);
    }
  }

  public void override(@Nullable MppJVMConfig config) {
    if (config == null) {
      return;
    }
    this.setInitHeapSize(config.getInitHeapSize());
    this.setMaxHeapSize(config.getMaxHeapSize());
    this.setMaxDirectMemorySize(config.getMaxDirectMemorySize());
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final MppJVMConfig config;

    public Builder() {
      this.config = new MppJVMConfig();
    }

    public Builder setInitHeapSize(int size) {
      this.config.setInitHeapSize(size);
      return this;
    }

    public Builder setMaxHeapSize(int size) {
      this.config.setMaxHeapSize(size);
      return this;
    }

    public Builder setMaxDirectMemorySize(int size) {
      this.config.setMaxDirectMemorySize(size);
      return this;
    }

    public MppJVMConfig build() {
      config.validate();
      return config;
    }
  }
}
