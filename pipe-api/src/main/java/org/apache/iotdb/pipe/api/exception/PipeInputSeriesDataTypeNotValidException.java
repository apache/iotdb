package org.apache.iotdb.pipe.api.exception;

import org.apache.iotdb.udf.api.type.Type;

import java.util.Arrays;

public class PipeInputSeriesDataTypeNotValidException extends PipeParameterNotValidException {

  public PipeInputSeriesDataTypeNotValidException(int index, Type actual, Type expected) {
    super(
        String.format(
            "the data type of the input series (index: %d) is not valid. expected: %s. actual: %s.",
            index, expected, actual));
  }

  public PipeInputSeriesDataTypeNotValidException(int index, Type actual, Type... expected) {
    super(
        String.format(
            "the data type of the input series (index: %d) is not valid. expected: %s. actual: %s.",
            index, Arrays.toString(expected), actual));
  }
}
