package org.apache.iotdb.db.utils.columngenerator.parameter;

import org.apache.iotdb.db.utils.columngenerator.ColumnGeneratorType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public abstract class ColumnGeneratorParameter {
  protected ColumnGeneratorType generatorType;

  public ColumnGeneratorParameter(ColumnGeneratorType generatorType) {
    this.generatorType = generatorType;
  }

  public ColumnGeneratorType getGeneratorType() {
    return generatorType;
  }

  protected abstract void serializeAttributes(ByteBuffer buffer);

  protected abstract void serializeAttributes(DataOutputStream stream) throws IOException;

  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(generatorType.getType(), buffer);
    serializeAttributes(buffer);
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(generatorType.getType(), stream);
    serializeAttributes(stream);
  }

  public abstract List<String> getColumnNames();

  public abstract List<TSDataType> getColumnTypes();

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ColumnGeneratorParameter)) {
      return false;
    }
    ColumnGeneratorParameter other = (ColumnGeneratorParameter) obj;
    return this.generatorType == other.generatorType;
  }

  @Override
  public int hashCode() {
    return generatorType.hashCode();
  }

  // Deserialize the parameter according to its type, currently we only support
  // SlidingTimeColumnGenerator used in GROUP BY TIME.
  public static ColumnGeneratorParameter deserialize(ByteBuffer byteBuffer) {
    byte type = ReadWriteIOUtils.readByte(byteBuffer);
    if (type == ColumnGeneratorType.SLIDING_TIME.getType()) {
      return SlidingTimeColumnGeneratorParameter.deserialize(byteBuffer);
    } else throw new UnsupportedOperationException("Unsupported ColumnGeneratorType: " + type);
  }
}
