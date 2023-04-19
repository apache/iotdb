package org.apache.iotdb.library.anomaly.util;

import java.util.ArrayList;
import java.util.Comparator;

public class KDTreeUtil {
  private Node root;

  private static class Node {
    ArrayList<Double> data;
    int depth;
    int index;
    Node left;
    Node right;

    Node(ArrayList<Double> data, int depth, int index) {
      this.data = data;
      this.depth = depth;
      this.index = index;
    }
  }

  public void buildTree(ArrayList<ArrayList<Double>> dataset) {
    if (dataset == null || dataset.isEmpty()) {
      throw new IllegalArgumentException("Dataset is null or empty");
    }
    root = buildTree(dataset, 0, dataset.size() - 1, 0);
  }

  private Node buildTree(ArrayList<ArrayList<Double>> dataset, int left, int right, int depth) {
    if (left > right) {
      return null;
    }
    int mid = (left + right) / 2;
    ArrayList<Double> data = dataset.get(mid);
    Node node = new Node(data, depth, mid);
    int nextDepth = (depth + 1) % data.size();
    node.left = buildTree(dataset, left, mid - 1, nextDepth);
    node.right = buildTree(dataset, mid + 1, right, nextDepth);
    return node;
  }

  public ArrayList<ArrayList<Double>> findKNearestNeighbors(ArrayList<Double> query, int k) {
    if (query == null || query.isEmpty()) {
      throw new IllegalArgumentException("Query is null or empty");
    }
    if (k <= 0) {
      throw new IllegalArgumentException("k must be a positive integer");
    }
    ArrayList<ArrayList<Double>> knnList = new ArrayList<>();
    findKNN(root, query, k, knnList);
    return knnList;
  }

  private void findKNN(
      Node node, ArrayList<Double> query, int k, ArrayList<ArrayList<Double>> knnList) {
    if (node == null) {
      return;
    }
    double dist = euclideanDistance(node.data, query);
    if (knnList.size() < k || dist < euclideanDistance(knnList.get(knnList.size() - 1), query)) {
      knnList.add(node.data);
      knnList.sort(Comparator.comparingDouble(p -> euclideanDistance(p, query)));
      if (knnList.size() > k) {
        knnList.remove(k);
      }
    }
    double axisDistance =
        node.data.get(node.depth % node.data.size()) - query.get(node.depth % node.data.size());
    Node nearerNode = (axisDistance <= 0) ? node.left : node.right;
    Node fartherNode = (axisDistance <= 0) ? node.right : node.left;
    findKNN(nearerNode, query, k, knnList);
    if (knnList.size() < k
        || Math.abs(axisDistance) < euclideanDistance(knnList.get(knnList.size() - 1), query)) {
      findKNN(fartherNode, query, k, knnList);
    }
  }

  private static double euclideanDistance(ArrayList<Double> p1, ArrayList<Double> p2) {
    if (p1 == null || p2 == null || p1.size() != p2.size()) {
      throw new IllegalArgumentException("Points are null or have different dimensions");
    }
    double sum = 0.0;
    for (int i = 0; i < p1.size(); i++) {
      double diff = p1.get(i) - p2.get(i);
      sum += diff * diff;
    }
    return Math.sqrt(sum);
  }

  public ArrayList<Double> findTheNearestNeighbor(ArrayList<Double> tuple) {
    if (tuple == null || tuple.isEmpty()) {
      throw new IllegalArgumentException("Tuple is null or empty");
    }
    return findNN(root, tuple, root.data);
  }

  private ArrayList<Double> findNN(Node node, ArrayList<Double> query, ArrayList<Double> nn) {
    if (node == null) {
      return nn;
    }
    double dist = euclideanDistance(node.data, query);
    if (dist < euclideanDistance(nn, query)) {
      nn = node.data;
    }
    double axisDistance =
        node.data.get(node.depth % node.data.size()) - query.get(node.depth % node.data.size());
    Node nearerNode = (axisDistance <= 0) ? node.left : node.right;
    Node fartherNode = (axisDistance <= 0) ? node.right : node.left;
    nn = findNN(nearerNode, query, nn);
    if (Math.abs(axisDistance) < euclideanDistance(nn, query)) {
      nn = findNN(fartherNode, query, nn);
    }
    return nn;
  }
}
