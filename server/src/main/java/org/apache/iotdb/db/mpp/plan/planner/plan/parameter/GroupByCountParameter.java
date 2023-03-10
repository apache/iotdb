package org.apache.iotdb.db.mpp.plan.planner.plan.parameter;

import org.apache.iotdb.db.mpp.execution.operator.window.WindowType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class GroupByCountParameter extends GroupByParameter {

  private final long countNumber;

  public GroupByCountParameter(long countNumber) {
    super(WindowType.COUNT_WINDOW);
    this.countNumber = countNumber;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(countNumber, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(countNumber, stream);
  }

  public long getCountNumber() {
    return countNumber;
  }

  public static GroupByParameter deserialize(ByteBuffer buffer) {
    long countNumber = ReadWriteIOUtils.readLong(buffer);
    return new GroupByCountParameter(countNumber);
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
    return this.countNumber == ((GroupByCountParameter) obj).getCountNumber();
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), countNumber);
  }
}
