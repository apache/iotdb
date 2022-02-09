package org.apache.iotdb.db.metadata.rocksdb;

public class RockDBConstants {

  protected static final char ZERO = '0';
  protected static final String ROOT = "r";

  protected static final String PATH_SEPARATOR = ".";
  protected static final String ESCAPE_PATH_SEPARATOR = "[.]";

  protected static final String TABLE_NAME_TAGS = "tags";

  // Node type
  protected static final byte NODE_TYPE_ROOT = 0x00;
  protected static final byte NODE_TYPE_INTERNAL = 0x01;
  protected static final byte NODE_TYPE_SG = 0x01 << 1;
  protected static final byte NODE_TYPE_ENTITY = 0x01 << 2;
  protected static final byte NODE_TYPE_MEASUREMENT = 0x01 << 3;
  protected static final byte NODE_TYPE_ALIAS = 0x01 << 4;
  protected static final byte NODE_TYPE_ENTITY_SG = NODE_TYPE_SG | NODE_TYPE_ENTITY;

  protected static final int MAX_NODE_TYPE_NUM = NODE_TYPE_ALIAS + 1;

  protected static final byte[] ALL_NODE_TYPE_ARRAY =
      new byte[] {
        NODE_TYPE_ROOT,
        NODE_TYPE_INTERNAL,
        NODE_TYPE_SG,
        NODE_TYPE_ENTITY,
        NODE_TYPE_MEASUREMENT,
        NODE_TYPE_ALIAS
      };

  protected static final byte DATA_VERSION = 0x00;

  protected static final byte DEFAULT_FLAG = 0x00;

  protected static final byte FLAG_SET_TTL = 0x01;
  protected static final byte FLAG_HAS_ALIAS = 0x01 << 1;
  protected static final byte FLAG_HAS_TAGS = 0x01 << 2;
  protected static final byte FLAG_HAS_ATTRIBUTES = 0x01 << 3;
  protected static final byte FLAG_IS_ALIGNED = 0x01 << 4;

  protected static final byte DATA_BLOCK_TYPE_TTL = 0x01;
  protected static final byte DATA_BLOCK_TYPE_SCHEMA = 0x01 << 1;
  protected static final byte DATA_BLOCK_TYPE_ALIAS = 0x01 << 2;
  protected static final byte DATA_BLOCK_TYPE_TAGS = 0x01 << 3;
  protected static final byte DATA_BLOCK_TYPE_ATTRIBUTES = 0x01 << 4;
  // alias's origin key
  protected static final byte DATA_BLOCK_TYPE_ORIGIN_KEY = 0x01 << 5;

  protected static final byte[] EMPTY_NODE_VALUE = new byte[] {0x00};
  protected static final byte[] DEFAULT_SG_NODE_VALUE = new byte[] {DATA_VERSION, DEFAULT_FLAG};
  protected static final byte[] DEFAULT_INTERNAL_NODE_VALUE =
      new byte[] {DATA_VERSION, DEFAULT_FLAG};
  protected static final byte[] DEFAULT_ENTITY_NODE_VALUE = new byte[] {DATA_VERSION, DEFAULT_FLAG};
}
