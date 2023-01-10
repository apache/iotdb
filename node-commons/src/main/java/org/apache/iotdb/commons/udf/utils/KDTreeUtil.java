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
package org.apache.iotdb.commons.udf.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Stack;

import static java.lang.Math.min;
import static java.lang.Math.sqrt;

public class KDTreeUtil {
  private Node kdTree;

  private static class Node {
    int partitionDimension;
    double partitionValue;
    ArrayList<Double> value;
    boolean isLeaf = false;
    Node left;
    Node right;
    //    min value of each dimension
    ArrayList<Double> min;
    //    max value of each dimension
    ArrayList<Double> max;
  }

  public static KDTreeUtil build(ArrayList<ArrayList<Double>> input, int dimension) {
    KDTreeUtil tree = new KDTreeUtil();
    tree.kdTree = new Node();
    tree.buildDetail(tree.kdTree, input, dimension);
    return tree;
  }

  private void buildDetail(Node node, ArrayList<ArrayList<Double>> data, int dimensions) {
    if (data.isEmpty()) {
      return;
    }
    if (data.size() == 1) {
      node.isLeaf = true;
      node.value = data.get(0);
      return;
    }
    node.partitionDimension = -1;
    double var = -1;
    double tmpvar;
    for (int i = 0; i < dimensions; i++) {
      tmpvar = UtilZ.variance(data, i);
      if (tmpvar > var) {
        var = tmpvar;
        node.partitionDimension = i;
      }
    }
    if (var == 0d) {
      node.isLeaf = true;
      node.value = data.get(0);
      return;
    }
    node.partitionValue = UtilZ.median(data, node.partitionDimension);

    ArrayList<ArrayList<Double>> maxMin = UtilZ.maxMin(data, dimensions);
    node.min = maxMin.get(0);
    node.max = maxMin.get(1);

    ArrayList<ArrayList<Double>> left = new ArrayList<>();
    ArrayList<ArrayList<Double>> right = new ArrayList<>();

    for (ArrayList<Double> d : data) {
      if (d.get(node.partitionDimension) < node.partitionValue) {
        left.add(d);
      } else if (d.get(node.partitionDimension) > node.partitionValue) {
        right.add(d);
      }
    }
    for (ArrayList<Double> d : data) {
      if (d.get(node.partitionDimension) == node.partitionValue) {
        if (left.isEmpty()) {
          left.add(d);
        } else {
          right.add(d);
        }
      }
    }

    Node leftNode = new Node();
    Node rightNode = new Node();
    node.left = leftNode;
    node.right = rightNode;
    buildDetail(leftNode, left, dimensions);
    buildDetail(rightNode, right, dimensions);
  }

  public ArrayList<Double> query(ArrayList<Double> input, double[] std) {
    Node node = kdTree;
    Stack<Node> stack = new Stack<>();
    while (!node.isLeaf) {
      if (input.get(node.partitionDimension) < node.partitionValue) {
        stack.add(node.right);
        node = node.left;
      } else {
        stack.push(node.left);
        node = node.right;
      }
    }

    double distance = UtilZ.distance(input, node.value, std);
    ArrayList<Double> nearest = queryRec(input, distance, stack, std);
    return nearest == null ? node.value : nearest;
  }

  public ArrayList<Double> queryRec(
      ArrayList<Double> input, double distance, Stack<Node> stack, double[] std) {
    ArrayList<Double> nearest = null;
    Node node;
    double tdis;
    while (!stack.isEmpty()) {
      node = stack.pop();
      if (node.isLeaf) {
        tdis = UtilZ.distance(input, node.value, std);
        if (tdis < distance) {
          distance = tdis;
          nearest = node.value;
        }
      } else {
        double minDistance = UtilZ.minDistance(input, node.max, node.min, std);
        if (minDistance < distance) {
          while (!node.isLeaf) {
            if (input.get(node.partitionDimension) < node.partitionValue) {
              stack.add(node.right);
              node = node.left;
            } else {
              stack.push(node.left);
              node = node.right;
            }
          }
          tdis = UtilZ.distance(input, node.value, std);
          if (tdis < distance) {
            distance = tdis;
            nearest = node.value;
          }
        }
      }
    }
    return nearest;
  }

  public ArrayList<ArrayList<Double>> queryRecKNN(
      ArrayList<Double> input, double distance, Stack<Node> stack, double[] std) {
    ArrayList<ArrayList<Double>> nearest = new ArrayList<>();
    Node node;
    double tdis;
    while (!stack.isEmpty()) {
      node = stack.pop();
      if (node.isLeaf) {
        tdis = UtilZ.distance(input, node.value, std);
        if (tdis < distance) {
          distance = tdis;
          nearest.add(node.value);
        }
      } else {
        double minDistance = UtilZ.minDistance(input, node.max, node.min, std);
        if (minDistance < distance) {
          while (!node.isLeaf) {
            if (input.get(node.partitionDimension) < node.partitionValue) {
              stack.add(node.right);
              node = node.left;
            } else {
              stack.push(node.left);
              node = node.right;
            }
          }
          tdis = UtilZ.distance(input, node.value, std);
          if (tdis < distance) {
            distance = tdis;
            nearest.add(node.value);
          }
        }
      }
    }
    return nearest;
  }

  public ArrayList<Double> findNearest(
      ArrayList<Double> input, ArrayList<ArrayList<Double>> nearest, double[] std) {
    double min_dis = Double.MAX_VALUE;
    int min_index = 0;
    for (int i = 0; i < nearest.size(); i++) {
      double dis = UtilZ.distance(input, nearest.get(i), std);
      if (dis < min_dis) {
        min_dis = dis;
        min_index = i;
      }
    }
    ArrayList<Double> nt = nearest.get(min_index);
    nearest.remove(min_index);
    return nt;
  }

  public ArrayList<ArrayList<Double>> queryKNN(ArrayList<Double> input, int k, double[] std) {
    ArrayList<ArrayList<Double>> kNearest = new ArrayList<>();
    Node node = kdTree;
    Stack<Node> stack = new Stack<>();
    while (!node.isLeaf) {
      if (input.get(node.partitionDimension) < node.partitionValue) {
        stack.add(node.right);
        node = node.left;
      } else {
        stack.push(node.left);
        node = node.right;
      }
    }
    double distance = UtilZ.distance(input, node.value, std);
    ArrayList<ArrayList<Double>> nearest = queryRecKNN(input, distance, stack, std);
    for (int i = 0; i < min(k, nearest.size()); i++) {
      kNearest.add(findNearest(input, nearest, std));
    }
    if (kNearest.isEmpty()) {
      kNearest.add(node.value);
    }
    for (ArrayList<Double> doubles : kNearest) {
      UtilZ.distance(doubles, input, std);
    }
    return kNearest;
  }

  private static class UtilZ {

    static double variance(ArrayList<ArrayList<Double>> data, int dimension) {
      double sum = 0d;
      for (ArrayList<Double> d : data) {
        sum += d.get(dimension);
      }
      double avg = sum / data.size();
      double ans = 0d;
      for (ArrayList<Double> d : data) {
        double temp = d.get(dimension) - avg;
        ans += temp * temp;
      }
      return ans / data.size();
    }

    static double median(ArrayList<ArrayList<Double>> data, int dimension) {
      ArrayList<Double> d = new ArrayList<>();
      for (ArrayList<Double> k : data) {
        d.add(k.get(dimension));
      }
      Collections.sort(d);
      int pos = d.size() / 2;
      return d.get(pos);
    }

    static ArrayList<ArrayList<Double>> maxMin(ArrayList<ArrayList<Double>> data, int dimensions) {
      ArrayList<ArrayList<Double>> mm = new ArrayList<>();
      ArrayList<Double> min_v = new ArrayList<>();
      ArrayList<Double> max_v = new ArrayList<>();
      for (int i = 0; i < dimensions; i++) {
        double min_temp = Double.MAX_VALUE;
        double max_temp = Double.MIN_VALUE;
        for (int j = 1; j < data.size(); j++) {
          ArrayList<Double> d = data.get(j);
          if (d.get(i) < min_temp) {
            min_temp = d.get(i);
          } else if (d.get(i) > max_temp) {
            max_temp = d.get(i);
          }
        }
        min_v.add(min_temp);
        max_v.add(max_temp);
      }
      mm.add(min_v);
      mm.add(max_v);
      return mm;
    }

    static double distance(ArrayList<Double> a, ArrayList<Double> b, double[] std) {
      double sum = 0d;
      for (int i = 0; i < a.size(); i++) {
        if (a.get(i) != null && b.get(i) != null)
          sum += Math.pow((a.get(i) - b.get(i)) / std[i], 2);
      }
      sum = sqrt(sum);
      return sum;
    }

    static double minDistance(
        ArrayList<Double> a, ArrayList<Double> max, ArrayList<Double> min, double[] std) {
      double sum = 0d;
      for (int i = 0; i < a.size(); i++) {
        if (a.get(i) > max.get(i)) sum += Math.pow((a.get(i) - max.get(i)) / std[i], 2);
        else if (a.get(i) < min.get(i)) {
          sum += Math.pow((min.get(i) - a.get(i)) / std[i], 2);
        }
      }
      sum = sqrt(sum);
      return sum;
    }
  }
}
