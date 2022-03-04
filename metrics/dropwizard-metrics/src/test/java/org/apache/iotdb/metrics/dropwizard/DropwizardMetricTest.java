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

package org.apache.iotdb.metrics.dropwizard;

import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.MetricService;

import java.util.*;

public class DropwizardMetricTest {
  private Integer metricNumberTotal;
  private Integer metricNameNumberLength;
  private Integer tagTotalNumber;
  private Integer tagSingleNumber;
  private Integer searchNumber;
  private String[] TAGS;
  private static Random random = new Random(43);
  private static MetricManager metricManager = MetricService.getMetricManager();
  private static Map<String, String[]> name2Tags = new HashMap<>();

  /**
   * @param metricNumber
   * @param tagTotalNumber
   * @param tagSingleNumber
   * @param searchNumber
   */
  DropwizardMetricTest(
      Integer metricNumber, Integer tagTotalNumber, Integer tagSingleNumber, Integer searchNumber) {
    this.metricNumberTotal = metricNumber;
    this.metricNameNumberLength = String.valueOf(metricNumberTotal).length();
    this.tagTotalNumber = tagTotalNumber;
    this.tagSingleNumber = tagSingleNumber;
    this.searchNumber = searchNumber;
    TAGS = new String[tagTotalNumber];
    for (int i = 0; i < tagTotalNumber; i++) {
      TAGS[i] = initTag(i);
    }
  }

  /**
   * generate tags for metric
   *
   * @param number
   * @return
   */
  private String initTag(Integer number) {
    StringBuilder stringBuilder = new StringBuilder(String.valueOf(number));
    while (stringBuilder.length() < 3) {
      stringBuilder.insert(0, '0');
    }
    stringBuilder.insert(0, "Tag");
    return stringBuilder.toString();
  }

  /**
   * generate name for metric
   *
   * @param number
   * @return
   */
  private String generateName(Integer number) {
    StringBuilder stringBuilder = new StringBuilder(String.valueOf(number));
    Integer length = String.valueOf(metricNumberTotal).length();
    while (stringBuilder.length() < metricNameNumberLength) {
      stringBuilder.insert(0, '0');
    }
    stringBuilder.insert(0, "counter");
    return stringBuilder.toString();
  }

  /**
   * generate tags of a metric
   *
   * @return
   */
  private String[] generateTags() {
    List<Integer> targets = new ArrayList<>();
    while (targets.size() < tagSingleNumber) {
      Integer target = generateRandom(tagTotalNumber);
      if (!targets.contains(target)) {
        targets.add(target);
      }
    }
    String[] tags = new String[tagSingleNumber];
    for (int i = 0; i < tagSingleNumber; i++) {
      tags[i] = TAGS[targets.get(i)];
    }
    return tags;
  }

  /**
   * generate next int
   *
   * @param max
   * @return
   */
  private Integer generateRandom(Integer max) {
    return random.nextInt(max);
  }

  /**
   * create metric in order
   *
   * @return
   */
  public long createMetricInorder() {
    long total = 0;
    for (int i = 0; i < metricNumberTotal; i++) {
      String name = generateName(i);
      String[] tags = generateTags();
      long start = System.currentTimeMillis();
      metricManager.getOrCreateCounter(name, tags);
      long stop = System.currentTimeMillis();
      total += (stop - start);
      name2Tags.put(name, tags);
    }
    return total;
  }

  /**
   * search metric in order
   *
   * @return
   */
  public long searchMetricInorder() {
    long total = 0;
    for (int i = 0; i < searchNumber; i++) {
      total += searchOne(i);
    }
    return total;
  }

  /**
   * search metric in random way
   *
   * @return
   */
  public long searchMetricDisorder() {
    long total = 0;
    for (int i = 0; i < searchNumber; i++) {
      total += searchOne(generateRandom(metricNumberTotal - 1));
    }
    return total;
  }

  private long searchOne(Integer target) {
    String name = generateName(target % metricNumberTotal);
    String[] tags = name2Tags.get(name);
    long start = System.currentTimeMillis();
    metricManager.getOrCreateCounter(name, tags);
    long stop = System.currentTimeMillis();
    return stop - start;
  }

  @Override
  public String toString() {
    return metricNumberTotal + "," + tagTotalNumber + "," + tagSingleNumber + "," + searchNumber;
  }

  public void stop() {
    name2Tags.clear();
    metricManager.stop();
  }
}
