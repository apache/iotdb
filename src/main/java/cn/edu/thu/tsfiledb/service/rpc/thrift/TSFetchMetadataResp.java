/**
 * Autogenerated by Thrift Compiler (0.9.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package cn.edu.thu.tsfiledb.service.rpc.thrift;

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
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2017-7-18")
public class TSFetchMetadataResp implements org.apache.thrift.TBase<TSFetchMetadataResp, TSFetchMetadataResp._Fields>, java.io.Serializable, Cloneable, Comparable<TSFetchMetadataResp> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TSFetchMetadataResp");

  private static final org.apache.thrift.protocol.TField STATUS_FIELD_DESC = new org.apache.thrift.protocol.TField("status", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField METADATA_IN_JSON_FIELD_DESC = new org.apache.thrift.protocol.TField("metadataInJson", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField DELTA_OBJECT_MAP_FIELD_DESC = new org.apache.thrift.protocol.TField("deltaObjectMap", org.apache.thrift.protocol.TType.MAP, (short)3);
  private static final org.apache.thrift.protocol.TField DATA_TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField("dataType", org.apache.thrift.protocol.TType.STRING, (short)4);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TSFetchMetadataRespStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TSFetchMetadataRespTupleSchemeFactory());
  }

  public TS_Status status; // required
  public String metadataInJson; // optional
  public Map<String,List<String>> deltaObjectMap; // optional
  public String dataType; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    STATUS((short)1, "status"),
    METADATA_IN_JSON((short)2, "metadataInJson"),
    DELTA_OBJECT_MAP((short)3, "deltaObjectMap"),
    DATA_TYPE((short)4, "dataType");

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
        case 1: // STATUS
          return STATUS;
        case 2: // METADATA_IN_JSON
          return METADATA_IN_JSON;
        case 3: // DELTA_OBJECT_MAP
          return DELTA_OBJECT_MAP;
        case 4: // DATA_TYPE
          return DATA_TYPE;
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
  private static final _Fields optionals[] = {_Fields.METADATA_IN_JSON,_Fields.DELTA_OBJECT_MAP,_Fields.DATA_TYPE};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.STATUS, new org.apache.thrift.meta_data.FieldMetaData("status", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TS_Status.class)));
    tmpMap.put(_Fields.METADATA_IN_JSON, new org.apache.thrift.meta_data.FieldMetaData("metadataInJson", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.DELTA_OBJECT_MAP, new org.apache.thrift.meta_data.FieldMetaData("deltaObjectMap", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)))));
    tmpMap.put(_Fields.DATA_TYPE, new org.apache.thrift.meta_data.FieldMetaData("dataType", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TSFetchMetadataResp.class, metaDataMap);
  }

  public TSFetchMetadataResp() {
  }

  public TSFetchMetadataResp(
    TS_Status status)
  {
    this();
    this.status = status;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TSFetchMetadataResp(TSFetchMetadataResp other) {
    if (other.isSetStatus()) {
      this.status = new TS_Status(other.status);
    }
    if (other.isSetMetadataInJson()) {
      this.metadataInJson = other.metadataInJson;
    }
    if (other.isSetDeltaObjectMap()) {
      Map<String,List<String>> __this__deltaObjectMap = new HashMap<String,List<String>>(other.deltaObjectMap.size());
      for (Map.Entry<String, List<String>> other_element : other.deltaObjectMap.entrySet()) {

        String other_element_key = other_element.getKey();
        List<String> other_element_value = other_element.getValue();

        String __this__deltaObjectMap_copy_key = other_element_key;

        List<String> __this__deltaObjectMap_copy_value = new ArrayList<String>(other_element_value);

        __this__deltaObjectMap.put(__this__deltaObjectMap_copy_key, __this__deltaObjectMap_copy_value);
      }
      this.deltaObjectMap = __this__deltaObjectMap;
    }
    if (other.isSetDataType()) {
      this.dataType = other.dataType;
    }
  }

  public TSFetchMetadataResp deepCopy() {
    return new TSFetchMetadataResp(this);
  }

  @Override
  public void clear() {
    this.status = null;
    this.metadataInJson = null;
    this.deltaObjectMap = null;
    this.dataType = null;
  }

  public TS_Status getStatus() {
    return this.status;
  }

  public TSFetchMetadataResp setStatus(TS_Status status) {
    this.status = status;
    return this;
  }

  public void unsetStatus() {
    this.status = null;
  }

  /** Returns true if field status is set (has been assigned a value) and false otherwise */
  public boolean isSetStatus() {
    return this.status != null;
  }

  public void setStatusIsSet(boolean value) {
    if (!value) {
      this.status = null;
    }
  }

  public String getMetadataInJson() {
    return this.metadataInJson;
  }

  public TSFetchMetadataResp setMetadataInJson(String metadataInJson) {
    this.metadataInJson = metadataInJson;
    return this;
  }

  public void unsetMetadataInJson() {
    this.metadataInJson = null;
  }

  /** Returns true if field metadataInJson is set (has been assigned a value) and false otherwise */
  public boolean isSetMetadataInJson() {
    return this.metadataInJson != null;
  }

  public void setMetadataInJsonIsSet(boolean value) {
    if (!value) {
      this.metadataInJson = null;
    }
  }

  public int getDeltaObjectMapSize() {
    return (this.deltaObjectMap == null) ? 0 : this.deltaObjectMap.size();
  }

  public void putToDeltaObjectMap(String key, List<String> val) {
    if (this.deltaObjectMap == null) {
      this.deltaObjectMap = new HashMap<String,List<String>>();
    }
    this.deltaObjectMap.put(key, val);
  }

  public Map<String,List<String>> getDeltaObjectMap() {
    return this.deltaObjectMap;
  }

  public TSFetchMetadataResp setDeltaObjectMap(Map<String,List<String>> deltaObjectMap) {
    this.deltaObjectMap = deltaObjectMap;
    return this;
  }

  public void unsetDeltaObjectMap() {
    this.deltaObjectMap = null;
  }

  /** Returns true if field deltaObjectMap is set (has been assigned a value) and false otherwise */
  public boolean isSetDeltaObjectMap() {
    return this.deltaObjectMap != null;
  }

  public void setDeltaObjectMapIsSet(boolean value) {
    if (!value) {
      this.deltaObjectMap = null;
    }
  }

  public String getDataType() {
    return this.dataType;
  }

  public TSFetchMetadataResp setDataType(String dataType) {
    this.dataType = dataType;
    return this;
  }

  public void unsetDataType() {
    this.dataType = null;
  }

  /** Returns true if field dataType is set (has been assigned a value) and false otherwise */
  public boolean isSetDataType() {
    return this.dataType != null;
  }

  public void setDataTypeIsSet(boolean value) {
    if (!value) {
      this.dataType = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case STATUS:
      if (value == null) {
        unsetStatus();
      } else {
        setStatus((TS_Status)value);
      }
      break;

    case METADATA_IN_JSON:
      if (value == null) {
        unsetMetadataInJson();
      } else {
        setMetadataInJson((String)value);
      }
      break;

    case DELTA_OBJECT_MAP:
      if (value == null) {
        unsetDeltaObjectMap();
      } else {
        setDeltaObjectMap((Map<String,List<String>>)value);
      }
      break;

    case DATA_TYPE:
      if (value == null) {
        unsetDataType();
      } else {
        setDataType((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case STATUS:
      return getStatus();

    case METADATA_IN_JSON:
      return getMetadataInJson();

    case DELTA_OBJECT_MAP:
      return getDeltaObjectMap();

    case DATA_TYPE:
      return getDataType();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case STATUS:
      return isSetStatus();
    case METADATA_IN_JSON:
      return isSetMetadataInJson();
    case DELTA_OBJECT_MAP:
      return isSetDeltaObjectMap();
    case DATA_TYPE:
      return isSetDataType();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TSFetchMetadataResp)
      return this.equals((TSFetchMetadataResp)that);
    return false;
  }

  public boolean equals(TSFetchMetadataResp that) {
    if (that == null)
      return false;

    boolean this_present_status = true && this.isSetStatus();
    boolean that_present_status = true && that.isSetStatus();
    if (this_present_status || that_present_status) {
      if (!(this_present_status && that_present_status))
        return false;
      if (!this.status.equals(that.status))
        return false;
    }

    boolean this_present_metadataInJson = true && this.isSetMetadataInJson();
    boolean that_present_metadataInJson = true && that.isSetMetadataInJson();
    if (this_present_metadataInJson || that_present_metadataInJson) {
      if (!(this_present_metadataInJson && that_present_metadataInJson))
        return false;
      if (!this.metadataInJson.equals(that.metadataInJson))
        return false;
    }

    boolean this_present_deltaObjectMap = true && this.isSetDeltaObjectMap();
    boolean that_present_deltaObjectMap = true && that.isSetDeltaObjectMap();
    if (this_present_deltaObjectMap || that_present_deltaObjectMap) {
      if (!(this_present_deltaObjectMap && that_present_deltaObjectMap))
        return false;
      if (!this.deltaObjectMap.equals(that.deltaObjectMap))
        return false;
    }

    boolean this_present_dataType = true && this.isSetDataType();
    boolean that_present_dataType = true && that.isSetDataType();
    if (this_present_dataType || that_present_dataType) {
      if (!(this_present_dataType && that_present_dataType))
        return false;
      if (!this.dataType.equals(that.dataType))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_status = true && (isSetStatus());
    list.add(present_status);
    if (present_status)
      list.add(status);

    boolean present_metadataInJson = true && (isSetMetadataInJson());
    list.add(present_metadataInJson);
    if (present_metadataInJson)
      list.add(metadataInJson);

    boolean present_deltaObjectMap = true && (isSetDeltaObjectMap());
    list.add(present_deltaObjectMap);
    if (present_deltaObjectMap)
      list.add(deltaObjectMap);

    boolean present_dataType = true && (isSetDataType());
    list.add(present_dataType);
    if (present_dataType)
      list.add(dataType);

    return list.hashCode();
  }

  @Override
  public int compareTo(TSFetchMetadataResp other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetStatus()).compareTo(other.isSetStatus());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStatus()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.status, other.status);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetMetadataInJson()).compareTo(other.isSetMetadataInJson());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMetadataInJson()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.metadataInJson, other.metadataInJson);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetDeltaObjectMap()).compareTo(other.isSetDeltaObjectMap());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDeltaObjectMap()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.deltaObjectMap, other.deltaObjectMap);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetDataType()).compareTo(other.isSetDataType());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDataType()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.dataType, other.dataType);
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
    StringBuilder sb = new StringBuilder("TSFetchMetadataResp(");
    boolean first = true;

    sb.append("status:");
    if (this.status == null) {
      sb.append("null");
    } else {
      sb.append(this.status);
    }
    first = false;
    if (isSetMetadataInJson()) {
      if (!first) sb.append(", ");
      sb.append("metadataInJson:");
      if (this.metadataInJson == null) {
        sb.append("null");
      } else {
        sb.append(this.metadataInJson);
      }
      first = false;
    }
    if (isSetDeltaObjectMap()) {
      if (!first) sb.append(", ");
      sb.append("deltaObjectMap:");
      if (this.deltaObjectMap == null) {
        sb.append("null");
      } else {
        sb.append(this.deltaObjectMap);
      }
      first = false;
    }
    if (isSetDataType()) {
      if (!first) sb.append(", ");
      sb.append("dataType:");
      if (this.dataType == null) {
        sb.append("null");
      } else {
        sb.append(this.dataType);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (status == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'status' was not present! Struct: " + toString());
    }
    // check for sub-struct validity
    if (status != null) {
      status.validate();
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
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TSFetchMetadataRespStandardSchemeFactory implements SchemeFactory {
    public TSFetchMetadataRespStandardScheme getScheme() {
      return new TSFetchMetadataRespStandardScheme();
    }
  }

  private static class TSFetchMetadataRespStandardScheme extends StandardScheme<TSFetchMetadataResp> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TSFetchMetadataResp struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // STATUS
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.status = new TS_Status();
              struct.status.read(iprot);
              struct.setStatusIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // METADATA_IN_JSON
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.metadataInJson = iprot.readString();
              struct.setMetadataInJsonIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // DELTA_OBJECT_MAP
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map124 = iprot.readMapBegin();
                struct.deltaObjectMap = new HashMap<String,List<String>>(2*_map124.size);
                String _key125;
                List<String> _val126;
                for (int _i127 = 0; _i127 < _map124.size; ++_i127)
                {
                  _key125 = iprot.readString();
                  {
                    org.apache.thrift.protocol.TList _list128 = iprot.readListBegin();
                    _val126 = new ArrayList<String>(_list128.size);
                    String _elem129;
                    for (int _i130 = 0; _i130 < _list128.size; ++_i130)
                    {
                      _elem129 = iprot.readString();
                      _val126.add(_elem129);
                    }
                    iprot.readListEnd();
                  }
                  struct.deltaObjectMap.put(_key125, _val126);
                }
                iprot.readMapEnd();
              }
              struct.setDeltaObjectMapIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // DATA_TYPE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.dataType = iprot.readString();
              struct.setDataTypeIsSet(true);
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
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, TSFetchMetadataResp struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.status != null) {
        oprot.writeFieldBegin(STATUS_FIELD_DESC);
        struct.status.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.metadataInJson != null) {
        if (struct.isSetMetadataInJson()) {
          oprot.writeFieldBegin(METADATA_IN_JSON_FIELD_DESC);
          oprot.writeString(struct.metadataInJson);
          oprot.writeFieldEnd();
        }
      }
      if (struct.deltaObjectMap != null) {
        if (struct.isSetDeltaObjectMap()) {
          oprot.writeFieldBegin(DELTA_OBJECT_MAP_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.LIST, struct.deltaObjectMap.size()));
            for (Map.Entry<String, List<String>> _iter131 : struct.deltaObjectMap.entrySet())
            {
              oprot.writeString(_iter131.getKey());
              {
                oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, _iter131.getValue().size()));
                for (String _iter132 : _iter131.getValue())
                {
                  oprot.writeString(_iter132);
                }
                oprot.writeListEnd();
              }
            }
            oprot.writeMapEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      if (struct.dataType != null) {
        if (struct.isSetDataType()) {
          oprot.writeFieldBegin(DATA_TYPE_FIELD_DESC);
          oprot.writeString(struct.dataType);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TSFetchMetadataRespTupleSchemeFactory implements SchemeFactory {
    public TSFetchMetadataRespTupleScheme getScheme() {
      return new TSFetchMetadataRespTupleScheme();
    }
  }

  private static class TSFetchMetadataRespTupleScheme extends TupleScheme<TSFetchMetadataResp> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TSFetchMetadataResp struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      struct.status.write(oprot);
      BitSet optionals = new BitSet();
      if (struct.isSetMetadataInJson()) {
        optionals.set(0);
      }
      if (struct.isSetDeltaObjectMap()) {
        optionals.set(1);
      }
      if (struct.isSetDataType()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetMetadataInJson()) {
        oprot.writeString(struct.metadataInJson);
      }
      if (struct.isSetDeltaObjectMap()) {
        {
          oprot.writeI32(struct.deltaObjectMap.size());
          for (Map.Entry<String, List<String>> _iter133 : struct.deltaObjectMap.entrySet())
          {
            oprot.writeString(_iter133.getKey());
            {
              oprot.writeI32(_iter133.getValue().size());
              for (String _iter134 : _iter133.getValue())
              {
                oprot.writeString(_iter134);
              }
            }
          }
        }
      }
      if (struct.isSetDataType()) {
        oprot.writeString(struct.dataType);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TSFetchMetadataResp struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.status = new TS_Status();
      struct.status.read(iprot);
      struct.setStatusIsSet(true);
      BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.metadataInJson = iprot.readString();
        struct.setMetadataInJsonIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TMap _map135 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.LIST, iprot.readI32());
          struct.deltaObjectMap = new HashMap<String,List<String>>(2*_map135.size);
          String _key136;
          List<String> _val137;
          for (int _i138 = 0; _i138 < _map135.size; ++_i138)
          {
            _key136 = iprot.readString();
            {
              org.apache.thrift.protocol.TList _list139 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
              _val137 = new ArrayList<String>(_list139.size);
              String _elem140;
              for (int _i141 = 0; _i141 < _list139.size; ++_i141)
              {
                _elem140 = iprot.readString();
                _val137.add(_elem140);
              }
            }
            struct.deltaObjectMap.put(_key136, _val137);
          }
        }
        struct.setDeltaObjectMapIsSet(true);
      }
      if (incoming.get(2)) {
        struct.dataType = iprot.readString();
        struct.setDataTypeIsSet(true);
      }
    }
  }

}

