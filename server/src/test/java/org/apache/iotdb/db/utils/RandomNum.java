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
package org.apache.iotdb.db.utils;

import java.util.Random;

public class RandomNum {

  private static Random random = new Random();

  private RandomNum() {
    throw new IllegalStateException("Utility class");
  }

  public static long getRandomLong(long min, long max) {
    return random.nextLong() % (max - min + 1) + min;
  }

  public static int getRandomInt(int min, int max) {
    return (random.nextInt(10000) % (max - min) + min);
  }

  /** get random float between min and max. */
  public static float getRandomFloat(float min, float max) {

    return (random.nextFloat() * (max - min) + min);
  }

  /** get random int between 0 and frequency. */
  public static int getAbnormalData(int frequency) {
    return random.nextInt() % frequency;
  }

  /**
   * get random text consisting of lowercase letters and numbers.
   *
   * @param length -the size of random text
   */
  public static String getRandomText(int length) {

    String base = "abcdefghijklmnopqrstuvwxyz0123456789";
    StringBuilder st = new StringBuilder();
    for (int i = 0; i < length; i++) {
      int number = random.nextInt(base.length());
      st = st.append(base.charAt(number));
    }
    return st.toString();
  }
}
