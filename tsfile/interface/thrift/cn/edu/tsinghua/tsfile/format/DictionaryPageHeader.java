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
 * TODO: *
 */
public class DictionaryPageHeader implements org.apache.thrift.TBase<DictionaryPageHeader, DictionaryPageHeader._Fields>, java.io.Serializable, Cloneable, Comparable<DictionaryPageHeader> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("DictionaryPageHeader");

  private static final org.apache.thrift.protocol.TField NUM_VALUES_FIELD_DESC = new org.apache.thrift.protocol.TField("num_values", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField ENCODING_FIELD_DESC = new org.apache.thrift.protocol.TField("encoding", org.apache.thrift.protocol.TType.I32, (short)2);
  private static final org.apache.thrift.protocol.TField IS_SORTED_FIELD_DESC = new org.apache.thrift.protocol.TField("is_sorted", org.apache.thrift.protocol.TType.BOOL, (short)3);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new DictionaryPageHeaderStandardSchemeFactory());
    schemes.put(TupleScheme.class, new DictionaryPageHeaderTupleSchemeFactory());
  }

  /**
   * Number of values in the dictionary *
   */
  public int num_values; // required
  /**
   * Encoding using this dictionary page *
   * 
   * @see Encoding
   */
  public Encoding encoding; // required
  /**
   * If true, the entries in the dictionary are sorted in ascending order *
   */
  public boolean is_sorted; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    /**
     * Number of values in the dictionary *
     */
    NUM_VALUES((short)1, "num_values"),
    /**
     * Encoding using this dictionary page *
     * 
     * @see Encoding
     */
    ENCODING((short)2, "encoding"),
    /**
     * If true, the entries in the dictionary are sorted in ascending order *
     */
    IS_SORTED((short)3, "is_sorted");

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
        case 2: // ENCODING
          return ENCODING;
        case 3: // IS_SORTED
          return IS_SORTED;
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
  private static final int __IS_SORTED_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  private _Fields optionals[] = {_Fields.IS_SORTED};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.NUM_VALUES, new org.apache.thrift.meta_data.FieldMetaData("num_values", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.ENCODING, new org.apache.thrift.meta_data.FieldMetaData("encoding", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, Encoding.class)));
    tmpMap.put(_Fields.IS_SORTED, new org.apache.thrift.meta_data.FieldMetaData("is_sorted", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(DictionaryPageHeader.class, metaDataMap);
  }

  public DictionaryPageHeader() {
  }

  public DictionaryPageHeader(
    int num_values,
    Encoding encoding)
  {
    this();
    this.num_values = num_values;
    setNum_valuesIsSet(true);
    this.encoding = encoding;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public DictionaryPageHeader(DictionaryPageHeader other) {
    __isset_bitfield = other.__isset_bitfield;
    this.num_values = other.num_values;
    if (other.isSetEncoding()) {
      this.encoding = other.encoding;
    }
    this.is_sorted = other.is_sorted;
  }

  public DictionaryPageHeader deepCopy() {
    return new DictionaryPageHeader(this);
  }

  @Override
  public void clear() {
    setNum_valuesIsSet(false);
    this.num_values = 0;
    this.encoding = null;
    setIs_sortedIsSet(false);
    this.is_sorted = false;
  }

  /**
   * Number of values in the dictionary *
   */
  public int getNum_values() {
    return this.num_values;
  }

  /**
   * Number of values in the dictionary *
   */
  public DictionaryPageHeader setNum_values(int num_values) {
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
   * Encoding using this dictionary page *
   * 
   * @see Encoding
   */
  public Encoding getEncoding() {
    return this.encoding;
  }

  /**
   * Encoding using this dictionary page *
   * 
   * @see Encoding
   */
  public DictionaryPageHeader setEncoding(Encoding encoding) {
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
   * If true, the entries in the dictionary are sorted in ascending order *
   */
  public boolean isIs_sorted() {
    return this.is_sorted;
  }

  /**
   * If true, the entries in the dictionary are sorted in ascending order *
   */
  public DictionaryPageHeader setIs_sorted(boolean is_sorted) {
    this.is_sorted = is_sorted;
    setIs_sortedIsSet(true);
    return this;
  }

  public void unsetIs_sorted() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __IS_SORTED_ISSET_ID);
  }

  /** Returns true if field is_sorted is set (has been assigned a value) and false otherwise */
  public boolean isSetIs_sorted() {
    return EncodingUtils.testBit(__isset_bitfield, __IS_SORTED_ISSET_ID);
  }

  public void setIs_sortedIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __IS_SORTED_ISSET_ID, value);
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

    case ENCODING:
      if (value == null) {
        unsetEncoding();
      } else {
        setEncoding((Encoding)value);
      }
      break;

    case IS_SORTED:
      if (value == null) {
        unsetIs_sorted();
      } else {
        setIs_sorted((Boolean)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case NUM_VALUES:
      return Integer.valueOf(getNum_values());

    case ENCODING:
      return getEncoding();

    case IS_SORTED:
      return Boolean.valueOf(isIs_sorted());

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
    case ENCODING:
      return isSetEncoding();
    case IS_SORTED:
      return isSetIs_sorted();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof DictionaryPageHeader)
      return this.equals((DictionaryPageHeader)that);
    return false;
  }

  public boolean equals(DictionaryPageHeader that) {
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

    boolean this_present_encoding = true && this.isSetEncoding();
    boolean that_present_encoding = true && that.isSetEncoding();
    if (this_present_encoding || that_present_encoding) {
      if (!(this_present_encoding && that_present_encoding))
        return false;
      if (!this.encoding.equals(that.encoding))
        return false;
    }

    boolean this_present_is_sorted = true && this.isSetIs_sorted();
    boolean that_present_is_sorted = true && that.isSetIs_sorted();
    if (this_present_is_sorted || that_present_is_sorted) {
      if (!(this_present_is_sorted && that_present_is_sorted))
        return false;
      if (this.is_sorted != that.is_sorted)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(DictionaryPageHeader other) {
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
    lastComparison = Boolean.valueOf(isSetIs_sorted()).compareTo(other.isSetIs_sorted());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetIs_sorted()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.is_sorted, other.is_sorted);
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
    StringBuilder sb = new StringBuilder("DictionaryPageHeader(");
    boolean first = true;

    sb.append("num_values:");
    sb.append(this.num_values);
    first = false;
    if (!first) sb.append(", ");
    sb.append("encoding:");
    if (this.encoding == null) {
      sb.append("null");
    } else {
      sb.append(this.encoding);
    }
    first = false;
    if (isSetIs_sorted()) {
      if (!first) sb.append(", ");
      sb.append("is_sorted:");
      sb.append(this.is_sorted);
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // alas, we cannot check 'num_values' because it's a primitive and you chose the non-beans generator.
    if (encoding == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'encoding' was not present! Struct: " + toString());
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

  private static class DictionaryPageHeaderStandardSchemeFactory implements SchemeFactory {
    public DictionaryPageHeaderStandardScheme getScheme() {
      return new DictionaryPageHeaderStandardScheme();
    }
  }

  private static class DictionaryPageHeaderStandardScheme extends StandardScheme<DictionaryPageHeader> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, DictionaryPageHeader struct) throws org.apache.thrift.TException {
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
          case 2: // ENCODING
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.encoding = Encoding.findByValue(iprot.readI32());
              struct.setEncodingIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // IS_SORTED
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.is_sorted = iprot.readBool();
              struct.setIs_sortedIsSet(true);
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
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, DictionaryPageHeader struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(NUM_VALUES_FIELD_DESC);
      oprot.writeI32(struct.num_values);
      oprot.writeFieldEnd();
      if (struct.encoding != null) {
        oprot.writeFieldBegin(ENCODING_FIELD_DESC);
        oprot.writeI32(struct.encoding.getValue());
        oprot.writeFieldEnd();
      }
      if (struct.isSetIs_sorted()) {
        oprot.writeFieldBegin(IS_SORTED_FIELD_DESC);
        oprot.writeBool(struct.is_sorted);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class DictionaryPageHeaderTupleSchemeFactory implements SchemeFactory {
    public DictionaryPageHeaderTupleScheme getScheme() {
      return new DictionaryPageHeaderTupleScheme();
    }
  }

  private static class DictionaryPageHeaderTupleScheme extends TupleScheme<DictionaryPageHeader> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, DictionaryPageHeader struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeI32(struct.num_values);
      oprot.writeI32(struct.encoding.getValue());
      BitSet optionals = new BitSet();
      if (struct.isSetIs_sorted()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetIs_sorted()) {
        oprot.writeBool(struct.is_sorted);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, DictionaryPageHeader struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.num_values = iprot.readI32();
      struct.setNum_valuesIsSet(true);
      struct.encoding = Encoding.findByValue(iprot.readI32());
      struct.setEncodingIsSet(true);
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        struct.is_sorted = iprot.readBool();
        struct.setIs_sortedIsSet(true);
      }
    }
  }

}

