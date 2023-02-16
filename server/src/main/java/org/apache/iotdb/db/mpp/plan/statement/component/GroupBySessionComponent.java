package org.apache.iotdb.db.mpp.plan.statement.component;

import org.apache.iotdb.db.mpp.execution.operator.window.WindowType;

public class GroupBySessionComponent extends GroupByComponent {

  private final long timeInterval;

  public GroupBySessionComponent(long timeInterval) {
    super(WindowType.SESSION_WINDOW);
    this.timeInterval = timeInterval;
  }

  public long getTimeInterval() {
    return timeInterval;
  }
}
