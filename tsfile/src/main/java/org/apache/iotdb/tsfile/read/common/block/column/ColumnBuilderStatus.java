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
package org.apache.iotdb.tsfile.read.common.block.column;

import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus;

import org.openjdk.jol.info.ClassLayout;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ColumnBuilderStatus {

  public static final int INSTANCE_SIZE = deepInstanceSize(ColumnBuilderStatus.class);

  private final TsBlockBuilderStatus tsBlockBuilderStatus;

  private int currentSize;

  public ColumnBuilderStatus(TsBlockBuilderStatus tsBlockBuilderStatus) {
    this.tsBlockBuilderStatus =
        requireNonNull(tsBlockBuilderStatus, "tsBlockBuilderStatus must not be null");
  }

  public int getMaxTsBlockSizeInBytes() {
    return tsBlockBuilderStatus.getMaxTsBlockSizeInBytes();
  }

  public void addBytes(int bytes) {
    currentSize += bytes;
    tsBlockBuilderStatus.addBytes(bytes);
  }

  @Override
  public String toString() {
    return "ColumnBuilderStatus{" + ", currentSize=" + currentSize + '}';
  }

  /**
   * Computes the size of an instance of this class assuming that all reference fields are non-null
   */
  private static int deepInstanceSize(Class<?> clazz) {
    if (clazz.isArray()) {
      throw new IllegalArgumentException(
          format(
              "Cannot determine size of %s because it contains an array", clazz.getSimpleName()));
    }
    if (clazz.isInterface()) {
      throw new IllegalArgumentException(format("%s is an interface", clazz.getSimpleName()));
    }
    if (Modifier.isAbstract(clazz.getModifiers())) {
      throw new IllegalArgumentException(format("%s is abstract", clazz.getSimpleName()));
    }
    if (!clazz.getSuperclass().equals(Object.class)) {
      throw new IllegalArgumentException(
          format(
              "Cannot determine size of a subclass. %s extends from %s",
              clazz.getSimpleName(), clazz.getSuperclass().getSimpleName()));
    }

    int size = ClassLayout.parseClass(clazz).instanceSize();
    for (Field field : clazz.getDeclaredFields()) {
      // if the field is not static and is a reference field and it's not synthetic
      if (!Modifier.isStatic(field.getModifiers())
          && !field.getType().isPrimitive()
          && !field.isSynthetic()) {
        size += deepInstanceSize(field.getType());
      }
    }
    return size;
  }
}
