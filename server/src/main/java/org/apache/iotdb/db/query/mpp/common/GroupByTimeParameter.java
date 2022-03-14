package org.apache.iotdb.db.query.mpp.common;

import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;

/**
 * In single-node IoTDB, the GroupByTimePlan is used to represent the parameter of `group by time`.
 * To avoid ambiguity, we use another name `GroupByTimeParameter` here
 */
public class GroupByTimeParameter extends GroupByTimePlan {
}
