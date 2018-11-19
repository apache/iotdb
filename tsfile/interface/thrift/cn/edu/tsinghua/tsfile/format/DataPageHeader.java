/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package cn.edu.tsinghua.tsfile.format;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data page header, with allowing reading information without decompressing the data
 * 
 */
public class DataPageHeader implements org.apache.thrift.TBase<DataPageHeader, DataPageHeader._Fields>, java.io.Serializable, Cloneable, Comparable<DataPageHeader> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("DataPageHeader");

  private static final org.apache.thrift.protocol.TField NUM_VALUES_FIELD_DESC = new org.apache.thrift.protocol.TField("num_values", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField NUM_ROWS_FIELD_DESC = new org.apache.thrift.protocol.TField("num_rows", org.apache.thrift.protocol.TType.I32, (short)2);
  private static final org.apache.thrift.protocol.TField ENCODING_FIELD_DESC = new org.apache.thrift.protocol.TField("encoding", org.apache.thrift.protocol.TType.I32, (short)3);
  private static final org.apache.thrift.protocol.TField DIGEST_FIELD_DESC = new org.apache.thrift.protocol.TField("digest", org.apache.thrift.protocol.TType.STRUCT, (short)4);
  private static final org.apache.thrift.protocol.TField IS_COMPRESSED_FIELD_DESC = new org.apache.thrift.protocol.TField("is_compressed", org.apache.thrift.protocol.TType.BOOL, (short)5);
  private static final org.apache.thrift.protocol.TField MAX_TIMESTAMP_FIELD_DESC = new org.apache.thrift.protocol.TField("max_timestamp", org.apache.thrift.protocol.TType.I64, (short)6);
  private static final org.apache.thrift.protocol.TField MIN_TIMESTAMP_FIELD_DESC = new org.apache.thrift.protocol.TField("min_timestamp", org.apache.thrift.protocol.TType.I64, (short)7);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new DataPageHeaderStandardSchemeFactory());
    schemes.put(TupleScheme.class, new DataPageHeaderTupleSchemeFactory());
  }

  /**
   * Number of values, including NULLs, in this data page. *
   */
  public int num_values; // required
  /**
   * Number of rows in this data page *
   */
  public int num_rows; // required
  /**
   * Encoding used for this data page *
   * 
   * @see Encoding
   */
  public Encoding encoding; // required
  /**
   * Optional digest/statistics for the data in this page*
   */
  public Digest digest; // optional
  /**
   * whether the values are compressed.
   * Which means the section of the page is compressed with the compression_type.
   * If missing it is considered compressed
   */
  public boolean is_compressed; // optional
  public long max_timestamp; // required
  public long min_timestamp; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    /**
     * Number of values, including NULLs, in this data page. *
     */
    NUM_VALUES((short)1, "num_values"),
    /**
     * Number of rows in this data page *
     */
    NUM_ROWS((short)2, "num_rows"),
    /**
     * Encoding used for this data page *
     * 
     * @see Encoding
     */
    ENCODING((short)3, "encoding"),
    /**
     * Optional digest/statistics for the data in this page*
     */
    DIGEST((short)4, "digest"),
    /**
     * whether the values are compressed.
     * Which means the section of the page is compressed with the compression_type.
     * If missing it is considered compressed
     */
    IS_COMPRESSED((short)5, "is_compressed"),
    MAX_TIMESTAMP((short)6, "max_timestamp"),
    MIN_TIMESTAMP((short)7, "min_timestamp");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // NUM_VALUES
          return NUM_VALUES;
        case 2: // NUM_ROWS
          return NUM_ROWS;
        case 3: // ENCODING
          return ENCODING;
        case 4: // DIGEST
          return DIGEST;
        case 5: // IS_COMPRESSED
          return IS_COMPRESSED;
        case 6: // MAX_TIMESTAMP
          return MAX_TIMESTAMP;
        case 7: // MIN_TIMESTAMP
          return MIN_TIMESTAMP;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __NUM_VALUES_ISSET_ID = 0;
  private static final int __NUM_ROWS_ISSET_ID = 1;
  private static final int __IS_COMPRESSED_ISSET_ID = 2;
  private static final int __MAX_TIMESTAMP_ISSET_ID = 3;
  private static final int __MIN_TIMESTAMP_ISSET_ID = 4;
  private byte __isset_bitfield = 0;
  private _Fields optionals[] = {_Fields.DIGEST,_Fields.IS_COMPRESSED};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.NUM_VALUES, new org.apache.thrift.meta_data.FieldMetaData("num_values", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.NUM_ROWS, new org.apache.thrift.meta_data.FieldMetaData("num_rows", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.ENCODING, new org.apache.thrift.meta_data.FieldMetaData("encoding", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, Encoding.class)));
    tmpMap.put(_Fields.DIGEST, new org.apache.thrift.meta_data.FieldMetaData("digest", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, Digest.class)));
    tmpMap.put(_Fields.IS_COMPRESSED, new org.apache.thrift.meta_data.FieldMetaData("is_compressed", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.MAX_TIMESTAMP, new org.apache.thrift.meta_data.FieldMetaData("max_timestamp", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.MIN_TIMESTAMP, new org.apache.thrift.meta_data.FieldMetaData("min_timestamp", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(DataPageHeader.class, metaDataMap);
  }

  public DataPageHeader() {
    this.is_compressed = true;

  }

  public DataPageHeader(
    int num_values,
    int num_rows,
    Encoding encoding,
    long max_timestamp,
    long min_timestamp)
  {
    this();
    this.num_values = num_values;
    setNum_valuesIsSet(true);
    this.num_rows = num_rows;
    setNum_rowsIsSet(true);
    this.encoding = encoding;
    this.max_timestamp = max_timestamp;
    setMax_timestampIsSet(true);
    this.min_timestamp = min_timestamp;
    setMin_timestampIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public DataPageHeader(DataPageHeader other) {
    __isset_bitfield = other.__isset_bitfield;
    this.num_values = other.num_values;
    this.num_rows = other.num_rows;
    if (other.isSetEncoding()) {
      this.encoding = other.encoding;
    }
    if (other.isSetDigest()) {
      this.digest = new Digest(other.digest);
    }
    this.is_compressed = other.is_compressed;
    this.max_timestamp = other.max_timestamp;
    this.min_timestamp = other.min_timestamp;
  }

  public DataPageHeader deepCopy() {
    return new DataPageHeader(this);
  }

  @Override
  public void clear() {
    setNum_valuesIsSet(false);
    this.num_values = 0;
    setNum_rowsIsSet(false);
    this.num_rows = 0;
    this.encoding = null;
    this.digest = null;
    this.is_compressed = true;

    setMax_timestampIsSet(false);
    this.max_timestamp = 0;
    setMin_timestampIsSet(false);
    this.min_timestamp = 0;
  }

  /**
   * Number of values, including NULLs, in this data page. *
   */
  public int getNum_values() {
    return this.num_values;
  }

  /**
   * Number of values, including NULLs, in this data page. *
   */
  public DataPageHeader setNum_values(int num_values) {
    this.num_values = num_values;
    setNum_valuesIsSet(true);
    return this;
  }

  public void unsetNum_values() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __NUM_VALUES_ISSET_ID);
  }

  /** Returns true if field num_values is set (has been assigned a value) and false otherwise */
  public boolean isSetNum_values() {
    return EncodingUtils.testBit(__isset_bitfield, __NUM_VALUES_ISSET_ID);
  }

  public void setNum_valuesIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __NUM_VALUES_ISSET_ID, value);
  }

  /**
   * Number of rows in this data page *
   */
  public int getNum_rows() {
    return this.num_rows;
  }

  /**
   * Number of rows in this data page *
   */
  public DataPageHeader setNum_rows(int num_rows) {
    this.num_rows = num_rows;
    setNum_rowsIsSet(true);
    return this;
  }

  public void unsetNum_rows() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __NUM_ROWS_ISSET_ID);
  }

  /** Returns true if field num_rows is set (has been assigned a value) and false otherwise */
  public boolean isSetNum_rows() {
    return EncodingUtils.testBit(__isset_bitfield, __NUM_ROWS_ISSET_ID);
  }

  public void setNum_rowsIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __NUM_ROWS_ISSET_ID, value);
  }

  /**
   * Encoding used for this data page *
   * 
   * @see Encoding
   */
  public Encoding getEncoding() {
    return this.encoding;
  }

  /**
   * Encoding used for this data page *
   * 
   * @see Encoding
   */
  public DataPageHeader setEncoding(Encoding encoding) {
    this.encoding = encoding;
    return this;
  }

  public void unsetEncoding() {
    this.encoding = null;
  }

  /** Returns true if field encoding is set (has been assigned a value) and false otherwise */
  public boolean isSetEncoding() {
    return this.encoding != null;
  }

  public void setEncodingIsSet(boolean value) {
    if (!value) {
      this.encoding = null;
    }
  }

  /**
   * Optional digest/statistics for the data in this page*
   */
  public Digest getDigest() {
    return this.digest;
  }

  /**
   * Optional digest/statistics for the data in this page*
   */
  public DataPageHeader setDigest(Digest digest) {
    this.digest = digest;
    return this;
  }

  public void unsetDigest() {
    this.digest = null;
  }

  /** Returns true if field digest is set (has been assigned a value) and false otherwise */
  public boolean isSetDigest() {
    return this.digest != null;
  }

  public void setDigestIsSet(boolean value) {
    if (!value) {
      this.digest = null;
    }
  }

  /**
   * whether the values are compressed.
   * Which means the section of the page is compressed with the compression_type.
   * If missing it is considered compressed
   */
  public boolean isIs_compressed() {
    return this.is_compressed;
  }

  /**
   * whether the values are compressed.
   * Which means the section of the page is compressed with the compression_type.
   * If missing it is considered compressed
   */
  public DataPageHeader setIs_compressed(boolean is_compressed) {
    this.is_compressed = is_compressed;
    setIs_compressedIsSet(true);
    return this;
  }

  public void unsetIs_compressed() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __IS_COMPRESSED_ISSET_ID);
  }

  /** Returns true if field is_compressed is set (has been assigned a value) and false otherwise */
  public boolean isSetIs_compressed() {
    return EncodingUtils.testBit(__isset_bitfield, __IS_COMPRESSED_ISSET_ID);
  }

  public void setIs_compressedIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __IS_COMPRESSED_ISSET_ID, value);
  }

  public long getMax_timestamp() {
    return this.max_timestamp;
  }

  public DataPageHeader setMax_timestamp(long max_timestamp) {
    this.max_timestamp = max_timestamp;
    setMax_timestampIsSet(true);
    return this;
  }

  public void unsetMax_timestamp() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __MAX_TIMESTAMP_ISSET_ID);
  }

  /** Returns true if field max_timestamp is set (has been assigned a value) and false otherwise */
  public boolean isSetMax_timestamp() {
    return EncodingUtils.testBit(__isset_bitfield, __MAX_TIMESTAMP_ISSET_ID);
  }

  public void setMax_timestampIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __MAX_TIMESTAMP_ISSET_ID, value);
  }

  public long getMin_timestamp() {
    return this.min_timestamp;
  }

  public DataPageHeader setMin_timestamp(long min_timestamp) {
    this.min_timestamp = min_timestamp;
    setMin_timestampIsSet(true);
    return this;
  }

  public void unsetMin_timestamp() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __MIN_TIMESTAMP_ISSET_ID);
  }

  /** Returns true if field min_timestamp is set (has been assigned a value) and false otherwise */
  public boolean isSetMin_timestamp() {
    return EncodingUtils.testBit(__isset_bitfield, __MIN_TIMESTAMP_ISSET_ID);
  }

  public void setMin_timestampIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __MIN_TIMESTAMP_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case NUM_VALUES:
      if (value == null) {
        unsetNum_values();
      } else {
        setNum_values((Integer)value);
      }
      break;

    case NUM_ROWS:
      if (value == null) {
        unsetNum_rows();
      } else {
        setNum_rows((Integer)value);
      }
      break;

    case ENCODING:
      if (value == null) {
        unsetEncoding();
      } else {
        setEncoding((Encoding)value);
      }
      break;

    case DIGEST:
      if (value == null) {
        unsetDigest();
      } else {
        setDigest((Digest)value);
      }
      break;

    case IS_COMPRESSED:
      if (value == null) {
        unsetIs_compressed();
      } else {
        setIs_compressed((Boolean)value);
      }
      break;

    case MAX_TIMESTAMP:
      if (value == null) {
        unsetMax_timestamp();
      } else {
        setMax_timestamp((Long)value);
      }
      break;

    case MIN_TIMESTAMP:
      if (value == null) {
        unsetMin_timestamp();
      } else {
        setMin_timestamp((Long)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case NUM_VALUES:
      return Integer.valueOf(getNum_values());

    case NUM_ROWS:
      return Integer.valueOf(getNum_rows());

    case ENCODING:
      return getEncoding();

    case DIGEST:
      return getDigest();

    case IS_COMPRESSED:
      return Boolean.valueOf(isIs_compressed());

    case MAX_TIMESTAMP:
      return Long.valueOf(getMax_timestamp());

    case MIN_TIMESTAMP:
      return Long.valueOf(getMin_timestamp());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case NUM_VALUES:
      return isSetNum_values();
    case NUM_ROWS:
      return isSetNum_rows();
    case ENCODING:
      return isSetEncoding();
    case DIGEST:
      return isSetDigest();
    case IS_COMPRESSED:
      return isSetIs_compressed();
    case MAX_TIMESTAMP:
      return isSetMax_timestamp();
    case MIN_TIMESTAMP:
      return isSetMin_timestamp();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof DataPageHeader)
      return this.equals((DataPageHeader)that);
    return false;
  }

  public boolean equals(DataPageHeader that) {
    if (that == null)
      return false;

    boolean this_present_num_values = true;
    boolean that_present_num_values = true;
    if (this_present_num_values || that_present_num_values) {
      if (!(this_present_num_values && that_present_num_values))
        return false;
      if (this.num_values != that.num_values)
        return false;
    }

    boolean this_present_num_rows = true;
    boolean that_present_num_rows = true;
    if (this_present_num_rows || that_present_num_rows) {
      if (!(this_present_num_rows && that_present_num_rows))
        return false;
      if (this.num_rows != that.num_rows)
        return false;
    }

    boolean this_present_encoding = true && this.isSetEncoding();
    boolean that_present_encoding = true && that.isSetEncoding();
    if (this_present_encoding || that_present_encoding) {
      if (!(this_present_encoding && that_present_encoding))
        return false;
      if (!this.encoding.equals(that.encoding))
        return false;
    }

    boolean this_present_digest = true && this.isSetDigest();
    boolean that_present_digest = true && that.isSetDigest();
    if (this_present_digest || that_present_digest) {
      if (!(this_present_digest && that_present_digest))
        return false;
      if (!this.digest.equals(that.digest))
        return false;
    }

    boolean this_present_is_compressed = true && this.isSetIs_compressed();
    boolean that_present_is_compressed = true && that.isSetIs_compressed();
    if (this_present_is_compressed || that_present_is_compressed) {
      if (!(this_present_is_compressed && that_present_is_compressed))
        return false;
      if (this.is_compressed != that.is_compressed)
        return false;
    }

    boolean this_present_max_timestamp = true;
    boolean that_present_max_timestamp = true;
    if (this_present_max_timestamp || that_present_max_timestamp) {
      if (!(this_present_max_timestamp && that_present_max_timestamp))
        return false;
      if (this.max_timestamp != that.max_timestamp)
        return false;
    }

    boolean this_present_min_timestamp = true;
    boolean that_present_min_timestamp = true;
    if (this_present_min_timestamp || that_present_min_timestamp) {
      if (!(this_present_min_timestamp && that_present_min_timestamp))
        return false;
      if (this.min_timestamp != that.min_timestamp)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(DataPageHeader other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetNum_values()).compareTo(other.isSetNum_values());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetNum_values()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.num_values, other.num_values);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetNum_rows()).compareTo(other.isSetNum_rows());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetNum_rows()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.num_rows, other.num_rows);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetEncoding()).compareTo(other.isSetEncoding());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetEncoding()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.encoding, other.encoding);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetDigest()).compareTo(other.isSetDigest());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDigest()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.digest, other.digest);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetIs_compressed()).compareTo(other.isSetIs_compressed());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetIs_compressed()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.is_compressed, other.is_compressed);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetMax_timestamp()).compareTo(other.isSetMax_timestamp());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMax_timestamp()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.max_timestamp, other.max_timestamp);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetMin_timestamp()).compareTo(other.isSetMin_timestamp());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMin_timestamp()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.min_timestamp, other.min_timestamp);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("DataPageHeader(");
    boolean first = true;

    sb.append("num_values:");
    sb.append(this.num_values);
    first = false;
    if (!first) sb.append(", ");
    sb.append("num_rows:");
    sb.append(this.num_rows);
    first = false;
    if (!first) sb.append(", ");
    sb.append("encoding:");
    if (this.encoding == null) {
      sb.append("null");
    } else {
      sb.append(this.encoding);
    }
    first = false;
    if (isSetDigest()) {
      if (!first) sb.append(", ");
      sb.append("digest:");
      if (this.digest == null) {
        sb.append("null");
      } else {
        sb.append(this.digest);
      }
      first = false;
    }
    if (isSetIs_compressed()) {
      if (!first) sb.append(", ");
      sb.append("is_compressed:");
      sb.append(this.is_compressed);
      first = false;
    }
    if (!first) sb.append(", ");
    sb.append("max_timestamp:");
    sb.append(this.max_timestamp);
    first = false;
    if (!first) sb.append(", ");
    sb.append("min_timestamp:");
    sb.append(this.min_timestamp);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // alas, we cannot check 'num_values' because it's a primitive and you chose the non-beans generator.
    // alas, we cannot check 'num_rows' because it's a primitive and you chose the non-beans generator.
    if (encoding == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'encoding' was not present! Struct: " + toString());
    }
    // alas, we cannot check 'max_timestamp' because it's a primitive and you chose the non-beans generator.
    // alas, we cannot check 'min_timestamp' because it's a primitive and you chose the non-beans generator.
    // check for sub-struct validity
    if (digest != null) {
      digest.validate();
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class DataPageHeaderStandardSchemeFactory implements SchemeFactory {
    public DataPageHeaderStandardScheme getScheme() {
      return new DataPageHeaderStandardScheme();
    }
  }

  private static class DataPageHeaderStandardScheme extends StandardScheme<DataPageHeader> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, DataPageHeader struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // NUM_VALUES
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.num_values = iprot.readI32();
              struct.setNum_valuesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // NUM_ROWS
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.num_rows = iprot.readI32();
              struct.setNum_rowsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // ENCODING
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.encoding = Encoding.findByValue(iprot.readI32());
              struct.setEncodingIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // DIGEST
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.digest = new Digest();
              struct.digest.read(iprot);
              struct.setDigestIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // IS_COMPRESSED
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.is_compressed = iprot.readBool();
              struct.setIs_compressedIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 6: // MAX_TIMESTAMP
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.max_timestamp = iprot.readI64();
              struct.setMax_timestampIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 7: // MIN_TIMESTAMP
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.min_timestamp = iprot.readI64();
              struct.setMin_timestampIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      if (!struct.isSetNum_values()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'num_values' was not found in serialized data! Struct: " + toString());
      }
      if (!struct.isSetNum_rows()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'num_rows' was not found in serialized data! Struct: " + toString());
      }
      if (!struct.isSetMax_timestamp()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'max_timestamp' was not found in serialized data! Struct: " + toString());
      }
      if (!struct.isSetMin_timestamp()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'min_timestamp' was not found in serialized data! Struct: " + toString());
      }
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, DataPageHeader struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(NUM_VALUES_FIELD_DESC);
      oprot.writeI32(struct.num_values);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(NUM_ROWS_FIELD_DESC);
      oprot.writeI32(struct.num_rows);
      oprot.writeFieldEnd();
      if (struct.encoding != null) {
        oprot.writeFieldBegin(ENCODING_FIELD_DESC);
        oprot.writeI32(struct.encoding.getValue());
        oprot.writeFieldEnd();
      }
      if (struct.digest != null) {
        if (struct.isSetDigest()) {
          oprot.writeFieldBegin(DIGEST_FIELD_DESC);
          struct.digest.write(oprot);
          oprot.writeFieldEnd();
        }
      }
      if (struct.isSetIs_compressed()) {
        oprot.writeFieldBegin(IS_COMPRESSED_FIELD_DESC);
        oprot.writeBool(struct.is_compressed);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(MAX_TIMESTAMP_FIELD_DESC);
      oprot.writeI64(struct.max_timestamp);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(MIN_TIMESTAMP_FIELD_DESC);
      oprot.writeI64(struct.min_timestamp);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class DataPageHeaderTupleSchemeFactory implements SchemeFactory {
    public DataPageHeaderTupleScheme getScheme() {
      return new DataPageHeaderTupleScheme();
    }
  }

  private static class DataPageHeaderTupleScheme extends TupleScheme<DataPageHeader> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, DataPageHeader struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeI32(struct.num_values);
      oprot.writeI32(struct.num_rows);
      oprot.writeI32(struct.encoding.getValue());
      oprot.writeI64(struct.max_timestamp);
      oprot.writeI64(struct.min_timestamp);
      BitSet optionals = new BitSet();
      if (struct.isSetDigest()) {
        optionals.set(0);
      }
      if (struct.isSetIs_compressed()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetDigest()) {
        struct.digest.write(oprot);
      }
      if (struct.isSetIs_compressed()) {
        oprot.writeBool(struct.is_compressed);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, DataPageHeader struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.num_values = iprot.readI32();
      struct.setNum_valuesIsSet(true);
      struct.num_rows = iprot.readI32();
      struct.setNum_rowsIsSet(true);
      struct.encoding = Encoding.findByValue(iprot.readI32());
      struct.setEncodingIsSet(true);
      struct.max_timestamp = iprot.readI64();
      struct.setMax_timestampIsSet(true);
      struct.min_timestamp = iprot.readI64();
      struct.setMin_timestampIsSet(true);
      BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        struct.digest = new Digest();
        struct.digest.read(iprot);
        struct.setDigestIsSet(true);
      }
      if (incoming.get(1)) {
        struct.is_compressed = iprot.readBool();
        struct.setIs_compressedIsSet(true);
      }
    }
  }

}

