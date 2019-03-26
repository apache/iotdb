package org.apache.iotdb.cluster.utils;


public class PhysicalNode {

  final String ip;
  final int port;

  PhysicalNode(String ip, int port) {
    this.ip = ip;
    this.port = port;
  }

  String getKey() {
    return String.format("%s:%d", ip, port);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((ip == null) ? 0 : ip.hashCode());
    result = prime * result + port;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    PhysicalNode other = (PhysicalNode) obj;
    if (this.port != other.port) {
      return false;
    }
    if (this.ip == null) {
      return other.ip == null ? true : false;
    }
    return this.ip.equals(other.ip);
  }

  @Override
  public String toString() {
    return getKey();
  }
}
