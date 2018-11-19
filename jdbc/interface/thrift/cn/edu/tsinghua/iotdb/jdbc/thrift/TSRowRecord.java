/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package cn.edu.tsinghua.iotdb.jdbc.thrift;

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

public class TSRowRecord implements org.apache.thrift.TBase<TSRowRecord, TSRowRecord._Fields>, java.io.Serializable, Cloneable, Comparable<TSRowRecord> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TSRowRecord");

  private static final org.apache.thrift.protocol.TField TIMESTAMP_FIELD_DESC = new org.apache.thrift.protocol.TField("timestamp", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField KEYS_FIELD_DESC = new org.apache.thrift.protocol.TField("keys", org.apache.thrift.protocol.TType.LIST, (short)2);
  private static final org.apache.thrift.protocol.TField VALUES_FIELD_DESC = new org.apache.thrift.protocol.TField("values", org.apache.thrift.protocol.TType.LIST, (short)3);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TSRowRecordStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TSRowRecordTupleSchemeFactory());
  }

  public long timestamp; // required
  public List<String> keys; // required
  public List<TSDataValue> values; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    TIMESTAMP((short)1, "timestamp"),
    KEYS((short)2, "keys"),
    VALUES((short)3, "values");

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
        case 1: // TIMESTAMP
          return TIMESTAMP;
        case 2: // KEYS
          return KEYS;
        case 3: // VALUES
          return VALUES;
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
  private static final int __TIMESTAMP_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.TIMESTAMP, new org.apache.thrift.meta_data.FieldMetaData("timestamp", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.KEYS, new org.apache.thrift.meta_data.FieldMetaData("keys", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.VALUES, new org.apache.thrift.meta_data.FieldMetaData("values", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TSDataValue.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TSRowRecord.class, metaDataMap);
  }

  public TSRowRecord() {
  }

  public TSRowRecord(
    long timestamp,
    List<String> keys,
    List<TSDataValue> values)
  {
    this();
    this.timestamp = timestamp;
    setTimestampIsSet(true);
    this.keys = keys;
    this.values = values;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TSRowRecord(TSRowRecord other) {
    __isset_bitfield = other.__isset_bitfield;
    this.timestamp = other.timestamp;
    if (other.isSetKeys()) {
      List<String> __this__keys = new ArrayList<String>(other.keys);
      this.keys = __this__keys;
    }
    if (other.isSetValues()) {
      List<TSDataValue> __this__values = new ArrayList<TSDataValue>(other.values.size());
      for (TSDataValue other_element : other.values) {
        __this__values.add(new TSDataValue(other_element));
      }
      this.values = __this__values;
    }
  }

  public TSRowRecord deepCopy() {
    return new TSRowRecord(this);
  }

  @Override
  public void clear() {
    setTimestampIsSet(false);
    this.timestamp = 0;
    this.keys = null;
    this.values = null;
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  public TSRowRecord setTimestamp(long timestamp) {
    this.timestamp = timestamp;
    setTimestampIsSet(true);
    return this;
  }

  public void unsetTimestamp() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __TIMESTAMP_ISSET_ID);
  }

  /** Returns true if field timestamp is set (has been assigned a value) and false otherwise */
  public boolean isSetTimestamp() {
    return EncodingUtils.testBit(__isset_bitfield, __TIMESTAMP_ISSET_ID);
  }

  public void setTimestampIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __TIMESTAMP_ISSET_ID, value);
  }

  public int getKeysSize() {
    return (this.keys == null) ? 0 : this.keys.size();
  }

  public java.util.Iterator<String> getKeysIterator() {
    return (this.keys == null) ? null : this.keys.iterator();
  }

  public void addToKeys(String elem) {
    if (this.keys == null) {
      this.keys = new ArrayList<String>();
    }
    this.keys.add(elem);
  }

  public List<String> getKeys() {
    return this.keys;
  }

  public TSRowRecord setKeys(List<String> keys) {
    this.keys = keys;
    return this;
  }

  public void unsetKeys() {
    this.keys = null;
  }

  /** Returns true if field keys is set (has been assigned a value) and false otherwise */
  public boolean isSetKeys() {
    return this.keys != null;
  }

  public void setKeysIsSet(boolean value) {
    if (!value) {
      this.keys = null;
    }
  }

  public int getValuesSize() {
    return (this.values == null) ? 0 : this.values.size();
  }

  public java.util.Iterator<TSDataValue> getValuesIterator() {
    return (this.values == null) ? null : this.values.iterator();
  }

  public void addToValues(TSDataValue elem) {
    if (this.values == null) {
      this.values = new ArrayList<TSDataValue>();
    }
    this.values.add(elem);
  }

  public List<TSDataValue> getValues() {
    return this.values;
  }

  public TSRowRecord setValues(List<TSDataValue> values) {
    this.values = values;
    return this;
  }

  public void unsetValues() {
    this.values = null;
  }

  /** Returns true if field values is set (has been assigned a value) and false otherwise */
  public boolean isSetValues() {
    return this.values != null;
  }

  public void setValuesIsSet(boolean value) {
    if (!value) {
      this.values = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case TIMESTAMP:
      if (value == null) {
        unsetTimestamp();
      } else {
        setTimestamp((Long)value);
      }
      break;

    case KEYS:
      if (value == null) {
        unsetKeys();
      } else {
        setKeys((List<String>)value);
      }
      break;

    case VALUES:
      if (value == null) {
        unsetValues();
      } else {
        setValues((List<TSDataValue>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case TIMESTAMP:
      return Long.valueOf(getTimestamp());

    case KEYS:
      return getKeys();

    case VALUES:
      return getValues();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case TIMESTAMP:
      return isSetTimestamp();
    case KEYS:
      return isSetKeys();
    case VALUES:
      return isSetValues();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TSRowRecord)
      return this.equals((TSRowRecord)that);
    return false;
  }

  public boolean equals(TSRowRecord that) {
    if (that == null)
      return false;

    boolean this_present_timestamp = true;
    boolean that_present_timestamp = true;
    if (this_present_timestamp || that_present_timestamp) {
      if (!(this_present_timestamp && that_present_timestamp))
        return false;
      if (this.timestamp != that.timestamp)
        return false;
    }

    boolean this_present_keys = true && this.isSetKeys();
    boolean that_present_keys = true && that.isSetKeys();
    if (this_present_keys || that_present_keys) {
      if (!(this_present_keys && that_present_keys))
        return false;
      if (!this.keys.equals(that.keys))
        return false;
    }

    boolean this_present_values = true && this.isSetValues();
    boolean that_present_values = true && that.isSetValues();
    if (this_present_values || that_present_values) {
      if (!(this_present_values && that_present_values))
        return false;
      if (!this.values.equals(that.values))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(TSRowRecord other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetTimestamp()).compareTo(other.isSetTimestamp());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTimestamp()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.timestamp, other.timestamp);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetKeys()).compareTo(other.isSetKeys());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetKeys()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.keys, other.keys);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetValues()).compareTo(other.isSetValues());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetValues()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.values, other.values);
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
    StringBuilder sb = new StringBuilder("TSRowRecord(");
    boolean first = true;

    sb.append("timestamp:");
    sb.append(this.timestamp);
    first = false;
    if (!first) sb.append(", ");
    sb.append("keys:");
    if (this.keys == null) {
      sb.append("null");
    } else {
      sb.append(this.keys);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("values:");
    if (this.values == null) {
      sb.append("null");
    } else {
      sb.append(this.values);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // alas, we cannot check 'timestamp' because it's a primitive and you chose the non-beans generator.
    if (keys == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'keys' was not present! Struct: " + toString());
    }
    if (values == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'values' was not present! Struct: " + toString());
    }
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

  private static class TSRowRecordStandardSchemeFactory implements SchemeFactory {
    public TSRowRecordStandardScheme getScheme() {
      return new TSRowRecordStandardScheme();
    }
  }

  private static class TSRowRecordStandardScheme extends StandardScheme<TSRowRecord> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TSRowRecord struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // TIMESTAMP
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.timestamp = iprot.readI64();
              struct.setTimestampIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // KEYS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list52 = iprot.readListBegin();
                struct.keys = new ArrayList<String>(_list52.size);
                for (int _i53 = 0; _i53 < _list52.size; ++_i53)
                {
                  String _elem54;
                  _elem54 = iprot.readString();
                  struct.keys.add(_elem54);
                }
                iprot.readListEnd();
              }
              struct.setKeysIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // VALUES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list55 = iprot.readListBegin();
                struct.values = new ArrayList<TSDataValue>(_list55.size);
                for (int _i56 = 0; _i56 < _list55.size; ++_i56)
                {
                  TSDataValue _elem57;
                  _elem57 = new TSDataValue();
                  _elem57.read(iprot);
                  struct.values.add(_elem57);
                }
                iprot.readListEnd();
              }
              struct.setValuesIsSet(true);
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
      if (!struct.isSetTimestamp()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'timestamp' was not found in serialized data! Struct: " + toString());
      }
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, TSRowRecord struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(TIMESTAMP_FIELD_DESC);
      oprot.writeI64(struct.timestamp);
      oprot.writeFieldEnd();
      if (struct.keys != null) {
        oprot.writeFieldBegin(KEYS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.keys.size()));
          for (String _iter58 : struct.keys)
          {
            oprot.writeString(_iter58);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.values != null) {
        oprot.writeFieldBegin(VALUES_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.values.size()));
          for (TSDataValue _iter59 : struct.values)
          {
            _iter59.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TSRowRecordTupleSchemeFactory implements SchemeFactory {
    public TSRowRecordTupleScheme getScheme() {
      return new TSRowRecordTupleScheme();
    }
  }

  private static class TSRowRecordTupleScheme extends TupleScheme<TSRowRecord> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TSRowRecord struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeI64(struct.timestamp);
      {
        oprot.writeI32(struct.keys.size());
        for (String _iter60 : struct.keys)
        {
          oprot.writeString(_iter60);
        }
      }
      {
        oprot.writeI32(struct.values.size());
        for (TSDataValue _iter61 : struct.values)
        {
          _iter61.write(oprot);
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TSRowRecord struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.timestamp = iprot.readI64();
      struct.setTimestampIsSet(true);
      {
        org.apache.thrift.protocol.TList _list62 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
        struct.keys = new ArrayList<String>(_list62.size);
        for (int _i63 = 0; _i63 < _list62.size; ++_i63)
        {
          String _elem64;
          _elem64 = iprot.readString();
          struct.keys.add(_elem64);
        }
      }
      struct.setKeysIsSet(true);
      {
        org.apache.thrift.protocol.TList _list65 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.values = new ArrayList<TSDataValue>(_list65.size);
        for (int _i66 = 0; _i66 < _list65.size; ++_i66)
        {
          TSDataValue _elem67;
          _elem67 = new TSDataValue();
          _elem67.read(iprot);
          struct.values.add(_elem67);
        }
      }
      struct.setValuesIsSet(true);
    }
  }

}

