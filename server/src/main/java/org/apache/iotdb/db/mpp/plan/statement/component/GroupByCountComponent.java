package org.apache.iotdb.db.mpp.plan.statement.component;

import org.apache.iotdb.db.mpp.execution.operator.window.WindowType;

public class GroupByCountComponent extends GroupByComponent {
  private final long countNumber;

  public GroupByCountComponent(long countNumber) {
    super(WindowType.COUNT_WINDOW);
    this.countNumber = countNumber;
  }

  public long getCountNumber() {
    return countNumber;
  }
}
