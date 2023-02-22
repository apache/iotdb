package org.apache.iotdb.pipe.api.exception;

public class PipeAttributeNotProvidedException extends PipeParameterNotValidException {

  public PipeAttributeNotProvidedException(String key) {
    super(String.format("attribute \"%s\" is required but was not provided.", key));
  }
}
