package org.apache.iotdb.db.mpp.plan.planner.plan.parameter;

import org.apache.iotdb.db.mpp.execution.operator.window.WindowType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class GroupBySessionParameter extends GroupByParameter {

  private final long timeInterval;

  public GroupBySessionParameter(long timeInterval) {
    super(WindowType.SESSION_WINDOW);
    this.timeInterval = timeInterval;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(timeInterval, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(timeInterval, stream);
  }

  public static GroupByParameter deserialize(ByteBuffer buffer) {
    long timeInterval = ReadWriteIOUtils.readLong(buffer);
    return new GroupBySessionParameter(timeInterval);
  }

  public long getTimeInterval() {
    return timeInterval;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    if (!super.equals(obj)) {
      return false;
    }
    return this.timeInterval == ((GroupBySessionParameter) obj).timeInterval;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), timeInterval);
  }
}
