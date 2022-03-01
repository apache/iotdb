package org.apache.iotdb.db.query.distribution.common;

/**
 * The traversal order for operators by timestamp
 */
public enum TraversalOrder {
    TIMESTAMP_ASC,
    TIMESTAMP_DESC,
    DEVICE_NAME_ASC,
    DEVICE_NAME_DESC,
}
