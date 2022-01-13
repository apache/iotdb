package org.apache.iotdb.db.query.executor;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;

public class IoTDBRelBuilder extends RelBuilder {

  public IoTDBRelBuilder(@Nullable Context context, RelOptCluster cluster, @Nullable RelOptSchema relOptSchema) {
    super(context, cluster, relOptSchema);
  }

}
