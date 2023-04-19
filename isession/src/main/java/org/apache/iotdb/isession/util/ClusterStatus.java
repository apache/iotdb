package org.apache.iotdb.isession.util;

/** Status of current cluster */
public enum ClusterStatus {

  /** cluster is up */
  PRIMARY_CLUSTER_UP,

  /** cluster is down, slave is up */
  SLAVE_CLUSTER_UP,

  /** cluster is down to up, slave is up to down */
  PRIMARY_CLUSTER_BE_READY;
}
