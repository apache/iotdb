package org.apache.iotdb.db.query.udf.builtin;

public class UDTFNonZeroDuration extends UDTFContinuouslySatisfy {

  @Override
  protected void setDefaultValue() {
    setDefaultMax(Long.MAX_VALUE);
    setDefaultMin(0L);
  }

  @Override
  protected Long getRecord() {
    return satisfyValueLastTime - satisfyValueStartTime;
  }

  @Override
  protected boolean satisfyInt(int value) {
    return value != 0;
  }

  @Override
  protected boolean satisfyLong(long value) {
    return value != 0L;
  }

  @Override
  protected boolean satisfyFloat(float value) {
    return value != 0f;
  }

  @Override
  protected boolean satisfyDouble(double value) {
    return value != 0.0;
  }

  @Override
  protected boolean satisfyBoolean(Boolean value) {
    return value;
  }
}
