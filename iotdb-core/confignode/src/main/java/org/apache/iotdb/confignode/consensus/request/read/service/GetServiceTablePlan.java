package org.apache.iotdb.confignode.consensus.request.read.service;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.read.ConfigPhysicalReadPlan;

public class GetServiceTablePlan extends ConfigPhysicalReadPlan {

  public GetServiceTablePlan() {
    super(ConfigPhysicalPlanType.GetServiceTable);
  }
}
