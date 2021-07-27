package org.apache.iotdb.cluster.log.applier;

import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleReplicaApplier extends DataLogApplier {
  private static final Logger logger = LoggerFactory.getLogger(SingleReplicaApplier.class);

  public SingleReplicaApplier(MetaGroupMember metaGroupMember, DataGroupMember dataGroupMember) {
    super(metaGroupMember, dataGroupMember);
  }

  @Override
  public void apply(Log log) {}

  public void applyPhysicalPlan(PhysicalPlan plan)
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException {
    if (plan instanceof InsertMultiTabletPlan) {
      applyInsert((InsertMultiTabletPlan) plan);
    } else if (plan instanceof InsertRowsPlan) {
      applyInsert((InsertRowsPlan) plan);
    } else if (plan instanceof InsertPlan) {
      applyInsert((InsertPlan) plan);
    } else {
      applyPhysicalPlan(plan, dataGroupMember);
    }
  }
}
