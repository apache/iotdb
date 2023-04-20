package org.apache.iotdb.isession.util;

/** Status of current cluster */
public enum ClusterStatus {

  /** primary cluster is up */
  PRIMARY_CLUSTER_UP,

  /** primary cluster is down, slave cluster is up */
  BACKUP_CLUSTER_UP,

  /** primary cluster recover, ready to start */
  PRIMARY_CLUSTER_BE_READY;
}
