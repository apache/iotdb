package org.apache.iotdb.db.metadata.plan.schemaregion.write;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.plan.schemaregion.ISchemaRegionPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.SchemaRegionPlanType;
import org.apache.iotdb.db.metadata.plan.schemaregion.SchemaRegionPlanVisitor;

import java.util.List;
import java.util.Map;

public interface IDeactivateTemplatePlan extends ISchemaRegionPlan {

  @Override
  default SchemaRegionPlanType getPlanType() {
    return SchemaRegionPlanType.DEACTIVATE_TEMPLATE;
  }

  @Override
  default <R, C> R accept(SchemaRegionPlanVisitor<R, C> visitor, C context) {
    return visitor.visitDeactivateTemplate(this, context);
  }

  Map<PartialPath, List<Integer>> getTemplateSetInfo();

  void setTemplateSetInfo(Map<PartialPath, List<Integer>> templateSetInfo);
}
