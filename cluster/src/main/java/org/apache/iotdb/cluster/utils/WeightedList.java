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

package org.apache.iotdb.cluster.utils;

import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class WeightedList<T> {
  private List<Pair<T, Double>> elements = new ArrayList<>();
  private Random random = new Random();
  private double[] probabilities;
  private double weightSum = 0.0;

  public void insert(T t, Double weight) {
    elements.add(new Pair<>(t, weight));
    weightSum += weight;
  }

  public List<T> select(int num) {
    List<T> rst = new ArrayList<>(num);
    if (num >= elements.size()) {
      for (Pair<T, Double> element : elements) {
        rst.add(element.left);
      }
      elements.clear();
      weightSum = 0.0;
    } else {
      for (int i = 0; i < num; i++) {
        rst.add(select());
      }
    }

    return rst;
  }

  public T select() {
    if (elements.isEmpty()) {
      return null;
    }

    if (probabilities == null || probabilities.length < elements.size()) {
      probabilities = new double[elements.size()];
    }

    probabilities[0] = elements.get(0).right / weightSum;
    for (int i = 1; i < elements.size(); i++) {
      probabilities[i] = elements.get(i).right / weightSum + probabilities[i - 1];
    }

    double p = random.nextDouble();
    int selectedIndex = elements.size() - 1;
    for (int i = 0; i < elements.size() - 1; i++) {
      if (p <= probabilities[i]) {
        selectedIndex = i;
        break;
      }
    }

    Pair<T, Double> rst = elements.remove(selectedIndex);
    weightSum -= rst.right;
    return rst.left;
  }

  public int size() {
    return elements.size();
  }
}
