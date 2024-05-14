package org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.request;

public enum PipeConsensusRequestVersion {
  VERSION_1((byte) 1),
  ;

  private final byte version;

  PipeConsensusRequestVersion(byte type) {
    this.version = type;
  }

  public byte getVersion() {
    return version;
  }
}
