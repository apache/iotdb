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
package org.apache.iotdb.tsfile.read.common;

import org.apache.iotdb.tsfile.utils.Binary;

public class DefaultArrayAllocator implements PrimitiveArrayAllocator {

  @Override
  public void release(int[] array) {

  }

  @Override
  public void release(long[] array) {

  }

  @Override
  public void release(float[] array) {

  }

  @Override
  public void release(double[] array) {

  }

  @Override
  public void release(Binary[] array) {

  }

  @Override
  public void release(boolean[] array) {

  }

  @Override
  public boolean[] allocBoolean(int size) {
    return new boolean[size];
  }

  @Override
  public int[] allocInt(int size) {
    return new int[size];
  }

  @Override
  public long[] allocLong(int size) {
    return new long[size];
  }

  @Override
  public float[] allocFloat(int size) {
    return new float[size];
  }

  @Override
  public double[] allocDouble(int size) {
    return new double[size];
  }

  @Override
  public Binary[] allocBinary(int size) {
    return new Binary[size];
  }
}
