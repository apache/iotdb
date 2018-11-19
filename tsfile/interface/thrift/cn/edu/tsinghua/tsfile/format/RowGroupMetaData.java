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

public class RowGroupMetaData implements org.apache.thrift.TBase<RowGroupMetaData, RowGroupMetaData._Fields>, java.io.Serializable, Cloneable, Comparable<RowGroupMetaData> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("RowGroupMetaData");

  private static final org.apache.thrift.protocol.TField TSC_METADATA_FIELD_DESC = new org.apache.thrift.protocol.TField("tsc_metadata", org.apache.thrift.protocol.TType.LIST, (short)1);
  private static final org.apache.thrift.protocol.TField DELTA_OBJECT_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("delta_object_id", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField TOTAL_BYTE_SIZE_FIELD_DESC = new org.apache.thrift.protocol.TField("total_byte_size", org.apache.thrift.protocol.TType.I64, (short)3);
  private static final org.apache.thrift.protocol.TField MAX_NUM_ROWS_FIELD_DESC = new org.apache.thrift.protocol.TField("max_num_rows", org.apache.thrift.protocol.TType.I64, (short)4);
  private static final org.apache.thrift.protocol.TField FILE_PATH_FIELD_DESC = new org.apache.thrift.protocol.TField("file_path", org.apache.thrift.protocol.TType.STRING, (short)5);
  private static final org.apache.thrift.protocol.TField DELTA_OBJECT_TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField("delta_object_type", org.apache.thrift.protocol.TType.STRING, (short)6);
  private static final org.apache.thrift.protocol.TField WRITTEN_TIME_FIELD_DESC = new org.apache.thrift.protocol.TField("writtenTime", org.apache.thrift.protocol.TType.I64, (short)7);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new RowGroupMetaDataStandardSchemeFactory());
    schemes.put(TupleScheme.class, new RowGroupMetaDataTupleSchemeFactory());
  }

  public List<TimeSeriesChunkMetaData> tsc_metadata; // required
  public String delta_object_id; // required
  /**
   * Total byte size of all the uncompressed time series data in this row group *
   */
  public long total_byte_size; // required
  /**
   * Maximum number of rows in this row group *
   */
  public long max_num_rows; // required
  /**
   * This path is relative to the current file. *
   */
  public String file_path; // optional
  public String delta_object_type; // required
  public long writtenTime; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    TSC_METADATA((short)1, "tsc_metadata"),
    DELTA_OBJECT_ID((short)2, "delta_object_id"),
    /**
     * Total byte size of all the uncompressed time series data in this row group *
     */
    TOTAL_BYTE_SIZE((short)3, "total_byte_size"),
    /**
     * Maximum number of rows in this row group *
     */
    MAX_NUM_ROWS((short)4, "max_num_rows"),
    /**
     * This path is relative to the current file. *
     */
    FILE_PATH((short)5, "file_path"),
    DELTA_OBJECT_TYPE((short)6, "delta_object_type"),
    WRITTEN_TIME((short)7, "writtenTime");

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
        case 1: // TSC_METADATA
          return TSC_METADATA;
        case 2: // DELTA_OBJECT_ID
          return DELTA_OBJECT_ID;
        case 3: // TOTAL_BYTE_SIZE
          return TOTAL_BYTE_SIZE;
        case 4: // MAX_NUM_ROWS
          return MAX_NUM_ROWS;
        case 5: // FILE_PATH
          return FILE_PATH;
        case 6: // DELTA_OBJECT_TYPE
          return DELTA_OBJECT_TYPE;
        case 7: // WRITTEN_TIME
          return WRITTEN_TIME;
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
  private static final int __TOTAL_BYTE_SIZE_ISSET_ID = 0;
  private static final int __MAX_NUM_ROWS_ISSET_ID = 1;
  private static final int __WRITTENTIME_ISSET_ID = 2;
  private byte __isset_bitfield = 0;
  private _Fields optionals[] = {_Fields.FILE_PATH};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.TSC_METADATA, new org.apache.thrift.meta_data.FieldMetaData("tsc_metadata", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TimeSeriesChunkMetaData.class))));
    tmpMap.put(_Fields.DELTA_OBJECT_ID, new org.apache.thrift.meta_data.FieldMetaData("delta_object_id", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.TOTAL_BYTE_SIZE, new org.apache.thrift.meta_data.FieldMetaData("total_byte_size", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.MAX_NUM_ROWS, new org.apache.thrift.meta_data.FieldMetaData("max_num_rows", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.FILE_PATH, new org.apache.thrift.meta_data.FieldMetaData("file_path", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.DELTA_OBJECT_TYPE, new org.apache.thrift.meta_data.FieldMetaData("delta_object_type", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.WRITTEN_TIME, new org.apache.thrift.meta_data.FieldMetaData("writtenTime", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(RowGroupMetaData.class, metaDataMap);
  }

  public RowGroupMetaData() {
  }

  public RowGroupMetaData(
    List<TimeSeriesChunkMetaData> tsc_metadata,
    String delta_object_id,
    long total_byte_size,
    long max_num_rows,
    String delta_object_type,
    long writtenTime)
  {
    this();
    this.tsc_metadata = tsc_metadata;
    this.delta_object_id = delta_object_id;
    this.total_byte_size = total_byte_size;
    setTotal_byte_sizeIsSet(true);
    this.max_num_rows = max_num_rows;
    setMax_num_rowsIsSet(true);
    this.delta_object_type = delta_object_type;
    this.writtenTime = writtenTime;
    setWrittenTimeIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public RowGroupMetaData(RowGroupMetaData other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetTsc_metadata()) {
      List<TimeSeriesChunkMetaData> __this__tsc_metadata = new ArrayList<TimeSeriesChunkMetaData>(other.tsc_metadata.size());
      for (TimeSeriesChunkMetaData other_element : other.tsc_metadata) {
        __this__tsc_metadata.add(new TimeSeriesChunkMetaData(other_element));
      }
      this.tsc_metadata = __this__tsc_metadata;
    }
    if (other.isSetDelta_object_id()) {
      this.delta_object_id = other.delta_object_id;
    }
    this.total_byte_size = other.total_byte_size;
    this.max_num_rows = other.max_num_rows;
    if (other.isSetFile_path()) {
      this.file_path = other.file_path;
    }
    if (other.isSetDelta_object_type()) {
      this.delta_object_type = other.delta_object_type;
    }
    this.writtenTime = other.writtenTime;
  }

  public RowGroupMetaData deepCopy() {
    return new RowGroupMetaData(this);
  }

  @Override
  public void clear() {
    this.tsc_metadata = null;
    this.delta_object_id = null;
    setTotal_byte_sizeIsSet(false);
    this.total_byte_size = 0;
    setMax_num_rowsIsSet(false);
    this.max_num_rows = 0;
    this.file_path = null;
    this.delta_object_type = null;
    setWrittenTimeIsSet(false);
    this.writtenTime = 0;
  }

  public int getTsc_metadataSize() {
    return (this.tsc_metadata == null) ? 0 : this.tsc_metadata.size();
  }

  public java.util.Iterator<TimeSeriesChunkMetaData> getTsc_metadataIterator() {
    return (this.tsc_metadata == null) ? null : this.tsc_metadata.iterator();
  }

  public void addToTsc_metadata(TimeSeriesChunkMetaData elem) {
    if (this.tsc_metadata == null) {
      this.tsc_metadata = new ArrayList<TimeSeriesChunkMetaData>();
    }
    this.tsc_metadata.add(elem);
  }

  public List<TimeSeriesChunkMetaData> getTsc_metadata() {
    return this.tsc_metadata;
  }

  public RowGroupMetaData setTsc_metadata(List<TimeSeriesChunkMetaData> tsc_metadata) {
    this.tsc_metadata = tsc_metadata;
    return this;
  }

  public void unsetTsc_metadata() {
    this.tsc_metadata = null;
  }

  /** Returns true if field tsc_metadata is set (has been assigned a value) and false otherwise */
  public boolean isSetTsc_metadata() {
    return this.tsc_metadata != null;
  }

  public void setTsc_metadataIsSet(boolean value) {
    if (!value) {
      this.tsc_metadata = null;
    }
  }

  public String getDelta_object_id() {
    return this.delta_object_id;
  }

  public RowGroupMetaData setDelta_object_id(String delta_object_id) {
    this.delta_object_id = delta_object_id;
    return this;
  }

  public void unsetDelta_object_id() {
    this.delta_object_id = null;
  }

  /** Returns true if field delta_object_id is set (has been assigned a value) and false otherwise */
  public boolean isSetDelta_object_id() {
    return this.delta_object_id != null;
  }

  public void setDelta_object_idIsSet(boolean value) {
    if (!value) {
      this.delta_object_id = null;
    }
  }

  /**
   * Total byte size of all the uncompressed time series data in this row group *
   */
  public long getTotal_byte_size() {
    return this.total_byte_size;
  }

  /**
   * Total byte size of all the uncompressed time series data in this row group *
   */
  public RowGroupMetaData setTotal_byte_size(long total_byte_size) {
    this.total_byte_size = total_byte_size;
    setTotal_byte_sizeIsSet(true);
    return this;
  }

  public void unsetTotal_byte_size() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __TOTAL_BYTE_SIZE_ISSET_ID);
  }

  /** Returns true if field total_byte_size is set (has been assigned a value) and false otherwise */
  public boolean isSetTotal_byte_size() {
    return EncodingUtils.testBit(__isset_bitfield, __TOTAL_BYTE_SIZE_ISSET_ID);
  }

  public void setTotal_byte_sizeIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __TOTAL_BYTE_SIZE_ISSET_ID, value);
  }

  /**
   * Maximum number of rows in this row group *
   */
  public long getMax_num_rows() {
    return this.max_num_rows;
  }

  /**
   * Maximum number of rows in this row group *
   */
  public RowGroupMetaData setMax_num_rows(long max_num_rows) {
    this.max_num_rows = max_num_rows;
    setMax_num_rowsIsSet(true);
    return this;
  }

  public void unsetMax_num_rows() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __MAX_NUM_ROWS_ISSET_ID);
  }

  /** Returns true if field max_num_rows is set (has been assigned a value) and false otherwise */
  public boolean isSetMax_num_rows() {
    return EncodingUtils.testBit(__isset_bitfield, __MAX_NUM_ROWS_ISSET_ID);
  }

  public void setMax_num_rowsIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __MAX_NUM_ROWS_ISSET_ID, value);
  }

  /**
   * This path is relative to the current file. *
   */
  public String getFile_path() {
    return this.file_path;
  }

  /**
   * This path is relative to the current file. *
   */
  public RowGroupMetaData setFile_path(String file_path) {
    this.file_path = file_path;
    return this;
  }

  public void unsetFile_path() {
    this.file_path = null;
  }

  /** Returns true if field file_path is set (has been assigned a value) and false otherwise */
  public boolean isSetFile_path() {
    return this.file_path != null;
  }

  public void setFile_pathIsSet(boolean value) {
    if (!value) {
      this.file_path = null;
    }
  }

  public String getDelta_object_type() {
    return this.delta_object_type;
  }

  public RowGroupMetaData setDelta_object_type(String delta_object_type) {
    this.delta_object_type = delta_object_type;
    return this;
  }

  public void unsetDelta_object_type() {
    this.delta_object_type = null;
  }

  /** Returns true if field delta_object_type is set (has been assigned a value) and false otherwise */
  public boolean isSetDelta_object_type() {
    return this.delta_object_type != null;
  }

  public void setDelta_object_typeIsSet(boolean value) {
    if (!value) {
      this.delta_object_type = null;
    }
  }

  public long getWrittenTime() {
    return this.writtenTime;
  }

  public RowGroupMetaData setWrittenTime(long writtenTime) {
    this.writtenTime = writtenTime;
    setWrittenTimeIsSet(true);
    return this;
  }

  public void unsetWrittenTime() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __WRITTENTIME_ISSET_ID);
  }

  /** Returns true if field writtenTime is set (has been assigned a value) and false otherwise */
  public boolean isSetWrittenTime() {
    return EncodingUtils.testBit(__isset_bitfield, __WRITTENTIME_ISSET_ID);
  }

  public void setWrittenTimeIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __WRITTENTIME_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case TSC_METADATA:
      if (value == null) {
        unsetTsc_metadata();
      } else {
        setTsc_metadata((List<TimeSeriesChunkMetaData>)value);
      }
      break;

    case DELTA_OBJECT_ID:
      if (value == null) {
        unsetDelta_object_id();
      } else {
        setDelta_object_id((String)value);
      }
      break;

    case TOTAL_BYTE_SIZE:
      if (value == null) {
        unsetTotal_byte_size();
      } else {
        setTotal_byte_size((Long)value);
      }
      break;

    case MAX_NUM_ROWS:
      if (value == null) {
        unsetMax_num_rows();
      } else {
        setMax_num_rows((Long)value);
      }
      break;

    case FILE_PATH:
      if (value == null) {
        unsetFile_path();
      } else {
        setFile_path((String)value);
      }
      break;

    case DELTA_OBJECT_TYPE:
      if (value == null) {
        unsetDelta_object_type();
      } else {
        setDelta_object_type((String)value);
      }
      break;

    case WRITTEN_TIME:
      if (value == null) {
        unsetWrittenTime();
      } else {
        setWrittenTime((Long)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case TSC_METADATA:
      return getTsc_metadata();

    case DELTA_OBJECT_ID:
      return getDelta_object_id();

    case TOTAL_BYTE_SIZE:
      return Long.valueOf(getTotal_byte_size());

    case MAX_NUM_ROWS:
      return Long.valueOf(getMax_num_rows());

    case FILE_PATH:
      return getFile_path();

    case DELTA_OBJECT_TYPE:
      return getDelta_object_type();

    case WRITTEN_TIME:
      return Long.valueOf(getWrittenTime());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case TSC_METADATA:
      return isSetTsc_metadata();
    case DELTA_OBJECT_ID:
      return isSetDelta_object_id();
    case TOTAL_BYTE_SIZE:
      return isSetTotal_byte_size();
    case MAX_NUM_ROWS:
      return isSetMax_num_rows();
    case FILE_PATH:
      return isSetFile_path();
    case DELTA_OBJECT_TYPE:
      return isSetDelta_object_type();
    case WRITTEN_TIME:
      return isSetWrittenTime();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof RowGroupMetaData)
      return this.equals((RowGroupMetaData)that);
    return false;
  }

  public boolean equals(RowGroupMetaData that) {
    if (that == null)
      return false;

    boolean this_present_tsc_metadata = true && this.isSetTsc_metadata();
    boolean that_present_tsc_metadata = true && that.isSetTsc_metadata();
    if (this_present_tsc_metadata || that_present_tsc_metadata) {
      if (!(this_present_tsc_metadata && that_present_tsc_metadata))
        return false;
      if (!this.tsc_metadata.equals(that.tsc_metadata))
        return false;
    }

    boolean this_present_delta_object_id = true && this.isSetDelta_object_id();
    boolean that_present_delta_object_id = true && that.isSetDelta_object_id();
    if (this_present_delta_object_id || that_present_delta_object_id) {
      if (!(this_present_delta_object_id && that_present_delta_object_id))
        return false;
      if (!this.delta_object_id.equals(that.delta_object_id))
        return false;
    }

    boolean this_present_total_byte_size = true;
    boolean that_present_total_byte_size = true;
    if (this_present_total_byte_size || that_present_total_byte_size) {
      if (!(this_present_total_byte_size && that_present_total_byte_size))
        return false;
      if (this.total_byte_size != that.total_byte_size)
        return false;
    }

    boolean this_present_max_num_rows = true;
    boolean that_present_max_num_rows = true;
    if (this_present_max_num_rows || that_present_max_num_rows) {
      if (!(this_present_max_num_rows && that_present_max_num_rows))
        return false;
      if (this.max_num_rows != that.max_num_rows)
        return false;
    }

    boolean this_present_file_path = true && this.isSetFile_path();
    boolean that_present_file_path = true && that.isSetFile_path();
    if (this_present_file_path || that_present_file_path) {
      if (!(this_present_file_path && that_present_file_path))
        return false;
      if (!this.file_path.equals(that.file_path))
        return false;
    }

    boolean this_present_delta_object_type = true && this.isSetDelta_object_type();
    boolean that_present_delta_object_type = true && that.isSetDelta_object_type();
    if (this_present_delta_object_type || that_present_delta_object_type) {
      if (!(this_present_delta_object_type && that_present_delta_object_type))
        return false;
      if (!this.delta_object_type.equals(that.delta_object_type))
        return false;
    }

    boolean this_present_writtenTime = true;
    boolean that_present_writtenTime = true;
    if (this_present_writtenTime || that_present_writtenTime) {
      if (!(this_present_writtenTime && that_present_writtenTime))
        return false;
      if (this.writtenTime != that.writtenTime)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(RowGroupMetaData other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetTsc_metadata()).compareTo(other.isSetTsc_metadata());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTsc_metadata()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.tsc_metadata, other.tsc_metadata);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetDelta_object_id()).compareTo(other.isSetDelta_object_id());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDelta_object_id()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.delta_object_id, other.delta_object_id);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTotal_byte_size()).compareTo(other.isSetTotal_byte_size());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTotal_byte_size()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.total_byte_size, other.total_byte_size);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetMax_num_rows()).compareTo(other.isSetMax_num_rows());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMax_num_rows()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.max_num_rows, other.max_num_rows);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetFile_path()).compareTo(other.isSetFile_path());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetFile_path()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.file_path, other.file_path);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetDelta_object_type()).compareTo(other.isSetDelta_object_type());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDelta_object_type()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.delta_object_type, other.delta_object_type);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetWrittenTime()).compareTo(other.isSetWrittenTime());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetWrittenTime()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.writtenTime, other.writtenTime);
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
    StringBuilder sb = new StringBuilder("RowGroupMetaData(");
    boolean first = true;

    sb.append("tsc_metadata:");
    if (this.tsc_metadata == null) {
      sb.append("null");
    } else {
      sb.append(this.tsc_metadata);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("delta_object_id:");
    if (this.delta_object_id == null) {
      sb.append("null");
    } else {
      sb.append(this.delta_object_id);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("total_byte_size:");
    sb.append(this.total_byte_size);
    first = false;
    if (!first) sb.append(", ");
    sb.append("max_num_rows:");
    sb.append(this.max_num_rows);
    first = false;
    if (isSetFile_path()) {
      if (!first) sb.append(", ");
      sb.append("file_path:");
      if (this.file_path == null) {
        sb.append("null");
      } else {
        sb.append(this.file_path);
      }
      first = false;
    }
    if (!first) sb.append(", ");
    sb.append("delta_object_type:");
    if (this.delta_object_type == null) {
      sb.append("null");
    } else {
      sb.append(this.delta_object_type);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("writtenTime:");
    sb.append(this.writtenTime);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (tsc_metadata == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'tsc_metadata' was not present! Struct: " + toString());
    }
    if (delta_object_id == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'delta_object_id' was not present! Struct: " + toString());
    }
    // alas, we cannot check 'total_byte_size' because it's a primitive and you chose the non-beans generator.
    // alas, we cannot check 'max_num_rows' because it's a primitive and you chose the non-beans generator.
    if (delta_object_type == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'delta_object_type' was not present! Struct: " + toString());
    }
    // alas, we cannot check 'writtenTime' because it's a primitive and you chose the non-beans generator.
    // check for sub-struct validity
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

  private static class RowGroupMetaDataStandardSchemeFactory implements SchemeFactory {
    public RowGroupMetaDataStandardScheme getScheme() {
      return new RowGroupMetaDataStandardScheme();
    }
  }

  private static class RowGroupMetaDataStandardScheme extends StandardScheme<RowGroupMetaData> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, RowGroupMetaData struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // TSC_METADATA
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list50 = iprot.readListBegin();
                struct.tsc_metadata = new ArrayList<TimeSeriesChunkMetaData>(_list50.size);
                for (int _i51 = 0; _i51 < _list50.size; ++_i51)
                {
                  TimeSeriesChunkMetaData _elem52;
                  _elem52 = new TimeSeriesChunkMetaData();
                  _elem52.read(iprot);
                  struct.tsc_metadata.add(_elem52);
                }
                iprot.readListEnd();
              }
              struct.setTsc_metadataIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // DELTA_OBJECT_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.delta_object_id = iprot.readString();
              struct.setDelta_object_idIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // TOTAL_BYTE_SIZE
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.total_byte_size = iprot.readI64();
              struct.setTotal_byte_sizeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // MAX_NUM_ROWS
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.max_num_rows = iprot.readI64();
              struct.setMax_num_rowsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // FILE_PATH
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.file_path = iprot.readString();
              struct.setFile_pathIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 6: // DELTA_OBJECT_TYPE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.delta_object_type = iprot.readString();
              struct.setDelta_object_typeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 7: // WRITTEN_TIME
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.writtenTime = iprot.readI64();
              struct.setWrittenTimeIsSet(true);
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
      if (!struct.isSetTotal_byte_size()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'total_byte_size' was not found in serialized data! Struct: " + toString());
      }
      if (!struct.isSetMax_num_rows()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'max_num_rows' was not found in serialized data! Struct: " + toString());
      }
      if (!struct.isSetWrittenTime()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'writtenTime' was not found in serialized data! Struct: " + toString());
      }
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, RowGroupMetaData struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.tsc_metadata != null) {
        oprot.writeFieldBegin(TSC_METADATA_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.tsc_metadata.size()));
          for (TimeSeriesChunkMetaData _iter53 : struct.tsc_metadata)
          {
            _iter53.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.delta_object_id != null) {
        oprot.writeFieldBegin(DELTA_OBJECT_ID_FIELD_DESC);
        oprot.writeString(struct.delta_object_id);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(TOTAL_BYTE_SIZE_FIELD_DESC);
      oprot.writeI64(struct.total_byte_size);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(MAX_NUM_ROWS_FIELD_DESC);
      oprot.writeI64(struct.max_num_rows);
      oprot.writeFieldEnd();
      if (struct.file_path != null) {
        if (struct.isSetFile_path()) {
          oprot.writeFieldBegin(FILE_PATH_FIELD_DESC);
          oprot.writeString(struct.file_path);
          oprot.writeFieldEnd();
        }
      }
      if (struct.delta_object_type != null) {
        oprot.writeFieldBegin(DELTA_OBJECT_TYPE_FIELD_DESC);
        oprot.writeString(struct.delta_object_type);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(WRITTEN_TIME_FIELD_DESC);
      oprot.writeI64(struct.writtenTime);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class RowGroupMetaDataTupleSchemeFactory implements SchemeFactory {
    public RowGroupMetaDataTupleScheme getScheme() {
      return new RowGroupMetaDataTupleScheme();
    }
  }

  private static class RowGroupMetaDataTupleScheme extends TupleScheme<RowGroupMetaData> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, RowGroupMetaData struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      {
        oprot.writeI32(struct.tsc_metadata.size());
        for (TimeSeriesChunkMetaData _iter54 : struct.tsc_metadata)
        {
          _iter54.write(oprot);
        }
      }
      oprot.writeString(struct.delta_object_id);
      oprot.writeI64(struct.total_byte_size);
      oprot.writeI64(struct.max_num_rows);
      oprot.writeString(struct.delta_object_type);
      oprot.writeI64(struct.writtenTime);
      BitSet optionals = new BitSet();
      if (struct.isSetFile_path()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetFile_path()) {
        oprot.writeString(struct.file_path);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, RowGroupMetaData struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TList _list55 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.tsc_metadata = new ArrayList<TimeSeriesChunkMetaData>(_list55.size);
        for (int _i56 = 0; _i56 < _list55.size; ++_i56)
        {
          TimeSeriesChunkMetaData _elem57;
          _elem57 = new TimeSeriesChunkMetaData();
          _elem57.read(iprot);
          struct.tsc_metadata.add(_elem57);
        }
      }
      struct.setTsc_metadataIsSet(true);
      struct.delta_object_id = iprot.readString();
      struct.setDelta_object_idIsSet(true);
      struct.total_byte_size = iprot.readI64();
      struct.setTotal_byte_sizeIsSet(true);
      struct.max_num_rows = iprot.readI64();
      struct.setMax_num_rowsIsSet(true);
      struct.delta_object_type = iprot.readString();
      struct.setDelta_object_typeIsSet(true);
      struct.writtenTime = iprot.readI64();
      struct.setWrittenTimeIsSet(true);
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        struct.file_path = iprot.readString();
        struct.setFile_pathIsSet(true);
      }
    }
  }

}

