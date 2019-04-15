package org.apache.iotdb.cluster.query.coordinatornode.manager;

import org.apache.iotdb.db.qp.physical.PhysicalPlan;

public interface IClusterQueryManager {

  void registerQuery(Long);

  PhysicalPlan getPhysicalPlan();
}
