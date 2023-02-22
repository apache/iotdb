package org.apache.iotdb.pipe.api.exception;

public class PipeInputSeriesIndexNotValidException extends PipeParameterNotValidException {
  public PipeInputSeriesIndexNotValidException(int providedIndex, int validIndexUpperBound) {
    super(
        String.format(
            "the index (%d) of the input series is not valid. valid index range: [0, %d).",
            providedIndex, validIndexUpperBound));
  }
}
