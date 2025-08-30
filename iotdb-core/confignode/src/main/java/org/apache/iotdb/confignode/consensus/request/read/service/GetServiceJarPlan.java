package org.apache.iotdb.confignode.consensus.request.read.service;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.read.ConfigPhysicalReadPlan;

import java.util.List;
import java.util.Objects;

public class GetServiceJarPlan extends ConfigPhysicalReadPlan {
  private final List<String> jarNames;

  public GetServiceJarPlan(List<String> jarNames) {
    super(ConfigPhysicalPlanType.GetServiceJar);
    this.jarNames = jarNames;
  }

  public List<String> getJarNames() {
    return jarNames;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    final GetServiceJarPlan that = (GetServiceJarPlan) o;
    return Objects.equals(jarNames, that.jarNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), jarNames);
  }
}
