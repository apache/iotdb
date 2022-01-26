package org.apache.iotdb.db.metadata.rocksdb;

public class RockDBConstants {

  protected static final char ZERO = '0';
  protected static final String ROOT = "r";

  protected static final String PATH_SEPARATOR = ".";
  protected static final String ESCAPE_PATH_SEPARATOR = "[.]";

  protected static final byte DATA_VERSION = 0x00;

  protected static final byte NODE_TYPE_ROOT = 0x00;
  protected static final byte NODE_TYPE_INNER = 0x01;
  protected static final byte NODE_TYPE_SG = 0x02;
  protected static final byte NODE_TYPE_ENTITY = 0x03;
  protected static final byte NODE_TYPE_MEASUREMENT = 0x04;
  protected static final byte NODE_TYPE_ENTITY_SG = 0x05;
  protected static final byte NODE_TYPE_ALIAS = 0x06;

  protected static final byte DEFAULT_FLAG = 0x00;
  protected static final byte FLAG_USING_TEMPLATE = 0x01;
  protected static final byte FLAG_SAVING_TEMPLATE = 0x01 << 1;
  protected static final byte FLAG_IS_ALIGNED = 0x01 << 2;
  protected static final byte FLAG_SET_TTL = 0x01 << 3;
  protected static final byte FLAG_HAS_ALIAS = 0x01 << 4;

  protected static final byte DATA_BLOCK_TYPE_TTL = 0x00;
  protected static final byte DATA_BLOCK_TYPE_SCHEMA = 0x01;
  protected static final byte DATA_BLOCK_TYPE_SCHEMA_TEMPLATE = 0x02;
  protected static final byte DATA_BLOCK_TYPE_ALIAS = 0x03;

  protected static final byte[] EMPTY_NODE_VALUE = new byte[] {0x00};
  protected static final byte[] DEFAULT_SG_NODE_VALUE =
      new byte[] {DATA_VERSION, NODE_TYPE_SG, DEFAULT_FLAG};
}
