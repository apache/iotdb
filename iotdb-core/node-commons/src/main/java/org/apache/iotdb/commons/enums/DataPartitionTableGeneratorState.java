package org.apache.iotdb.commons.enums;

public enum DataPartitionTableGeneratorState {
  SUCCESS(0),
  FAILED(1),
  IN_PROGRESS(2),
  UNKNOWN(-1);

  private final int code;

  DataPartitionTableGeneratorState(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  /**
   * get DataNodeRemoveState by code
   *
   * @param code code
   * @return DataNodeRemoveState
   */
  public static DataPartitionTableGeneratorState getStateByCode(int code) {
    for (DataPartitionTableGeneratorState state : DataPartitionTableGeneratorState.values()) {
      if (state.code == code) {
        return state;
      }
    }
    return UNKNOWN;
  }
}
