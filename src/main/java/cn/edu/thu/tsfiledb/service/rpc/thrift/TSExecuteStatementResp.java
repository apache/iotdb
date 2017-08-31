/**
 * Autogenerated by Thrift Compiler (0.9.1)
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TSExecuteStatementResp implements org.apache.thrift.TBase<TSExecuteStatementResp, TSExecuteStatementResp._Fields>, java.io.Serializable, Cloneable, Comparable<TSExecuteStatementResp> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TSExecuteStatementResp");

  private static final org.apache.thrift.protocol.TField STATUS_FIELD_DESC = new org.apache.thrift.protocol.TField("status", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField OPERATION_HANDLE_FIELD_DESC = new org.apache.thrift.protocol.TField("operationHandle", org.apache.thrift.protocol.TType.STRUCT, (short)2);
  private static final org.apache.thrift.protocol.TField COLUMNS_FIELD_DESC = new org.apache.thrift.protocol.TField("columns", org.apache.thrift.protocol.TType.LIST, (short)3);
  private static final org.apache.thrift.protocol.TField OPERATION_TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField("operationType", org.apache.thrift.protocol.TType.LIST, (short)4);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TSExecuteStatementRespStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TSExecuteStatementRespTupleSchemeFactory());
  }

  public TS_Status status; // required
  public TSOperationHandle operationHandle; // optional
  public List<String> columns; // optional
  public List<String> operationType; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    STATUS((short)1, "status"),
    OPERATION_HANDLE((short)2, "operationHandle"),
    COLUMNS((short)3, "columns"),
    OPERATION_TYPE((short)4, "operationType");

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
        case 2: // OPERATION_HANDLE
          return OPERATION_HANDLE;
        case 3: // COLUMNS
          return COLUMNS;
        case 4: // OPERATION_TYPE
          return OPERATION_TYPE;
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
  private _Fields optionals[] = {_Fields.OPERATION_HANDLE,_Fields.COLUMNS,_Fields.OPERATION_TYPE};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.STATUS, new org.apache.thrift.meta_data.FieldMetaData("status", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TS_Status.class)));
    tmpMap.put(_Fields.OPERATION_HANDLE, new org.apache.thrift.meta_data.FieldMetaData("operationHandle", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TSOperationHandle.class)));
    tmpMap.put(_Fields.COLUMNS, new org.apache.thrift.meta_data.FieldMetaData("columns", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.OPERATION_TYPE, new org.apache.thrift.meta_data.FieldMetaData("operationType", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TSExecuteStatementResp.class, metaDataMap);
  }

  public TSExecuteStatementResp() {
  }

  public TSExecuteStatementResp(
    TS_Status status)
  {
    this();
    this.status = status;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TSExecuteStatementResp(TSExecuteStatementResp other) {
    if (other.isSetStatus()) {
      this.status = new TS_Status(other.status);
    }
    if (other.isSetOperationHandle()) {
      this.operationHandle = new TSOperationHandle(other.operationHandle);
    }
    if (other.isSetColumns()) {
      List<String> __this__columns = new ArrayList<String>(other.columns);
      this.columns = __this__columns;
    }
    if (other.isSetOperationType()) {
      List<String> __this__operationType = new ArrayList<String>(other.operationType);
      this.operationType = __this__operationType;
    }
  }

  public TSExecuteStatementResp deepCopy() {
    return new TSExecuteStatementResp(this);
  }

  @Override
  public void clear() {
    this.status = null;
    this.operationHandle = null;
    this.columns = null;
    this.operationType = null;
  }

  public TS_Status getStatus() {
    return this.status;
  }

  public TSExecuteStatementResp setStatus(TS_Status status) {
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

  public TSOperationHandle getOperationHandle() {
    return this.operationHandle;
  }

  public TSExecuteStatementResp setOperationHandle(TSOperationHandle operationHandle) {
    this.operationHandle = operationHandle;
    return this;
  }

  public void unsetOperationHandle() {
    this.operationHandle = null;
  }

  /** Returns true if field operationHandle is set (has been assigned a value) and false otherwise */
  public boolean isSetOperationHandle() {
    return this.operationHandle != null;
  }

  public void setOperationHandleIsSet(boolean value) {
    if (!value) {
      this.operationHandle = null;
    }
  }

  public int getColumnsSize() {
    return (this.columns == null) ? 0 : this.columns.size();
  }

  public java.util.Iterator<String> getColumnsIterator() {
    return (this.columns == null) ? null : this.columns.iterator();
  }

  public void addToColumns(String elem) {
    if (this.columns == null) {
      this.columns = new ArrayList<String>();
    }
    this.columns.add(elem);
  }

  public List<String> getColumns() {
    return this.columns;
  }

  public TSExecuteStatementResp setColumns(List<String> columns) {
    this.columns = columns;
    return this;
  }

  public void unsetColumns() {
    this.columns = null;
  }

  /** Returns true if field columns is set (has been assigned a value) and false otherwise */
  public boolean isSetColumns() {
    return this.columns != null;
  }

  public void setColumnsIsSet(boolean value) {
    if (!value) {
      this.columns = null;
    }
  }

  public int getOperationTypeSize() {
    return (this.operationType == null) ? 0 : this.operationType.size();
  }

  public java.util.Iterator<String> getOperationTypeIterator() {
    return (this.operationType == null) ? null : this.operationType.iterator();
  }

  public void addToOperationType(String elem) {
    if (this.operationType == null) {
      this.operationType = new ArrayList<String>();
    }
    this.operationType.add(elem);
  }

  public List<String> getOperationType() {
    return this.operationType;
  }

  public TSExecuteStatementResp setOperationType(List<String> operationType) {
    this.operationType = operationType;
    return this;
  }

  public void unsetOperationType() {
    this.operationType = null;
  }

  /** Returns true if field operationType is set (has been assigned a value) and false otherwise */
  public boolean isSetOperationType() {
    return this.operationType != null;
  }

  public void setOperationTypeIsSet(boolean value) {
    if (!value) {
      this.operationType = null;
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

    case OPERATION_HANDLE:
      if (value == null) {
        unsetOperationHandle();
      } else {
        setOperationHandle((TSOperationHandle)value);
      }
      break;

    case COLUMNS:
      if (value == null) {
        unsetColumns();
      } else {
        setColumns((List<String>)value);
      }
      break;

    case OPERATION_TYPE:
      if (value == null) {
        unsetOperationType();
      } else {
        setOperationType((List<String>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case STATUS:
      return getStatus();

    case OPERATION_HANDLE:
      return getOperationHandle();

    case COLUMNS:
      return getColumns();

    case OPERATION_TYPE:
      return getOperationType();

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
    case OPERATION_HANDLE:
      return isSetOperationHandle();
    case COLUMNS:
      return isSetColumns();
    case OPERATION_TYPE:
      return isSetOperationType();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TSExecuteStatementResp)
      return this.equals((TSExecuteStatementResp)that);
    return false;
  }

  public boolean equals(TSExecuteStatementResp that) {
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

    boolean this_present_operationHandle = true && this.isSetOperationHandle();
    boolean that_present_operationHandle = true && that.isSetOperationHandle();
    if (this_present_operationHandle || that_present_operationHandle) {
      if (!(this_present_operationHandle && that_present_operationHandle))
        return false;
      if (!this.operationHandle.equals(that.operationHandle))
        return false;
    }

    boolean this_present_columns = true && this.isSetColumns();
    boolean that_present_columns = true && that.isSetColumns();
    if (this_present_columns || that_present_columns) {
      if (!(this_present_columns && that_present_columns))
        return false;
      if (!this.columns.equals(that.columns))
        return false;
    }

    boolean this_present_operationType = true && this.isSetOperationType();
    boolean that_present_operationType = true && that.isSetOperationType();
    if (this_present_operationType || that_present_operationType) {
      if (!(this_present_operationType && that_present_operationType))
        return false;
      if (!this.operationType.equals(that.operationType))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(TSExecuteStatementResp other) {
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
    lastComparison = Boolean.valueOf(isSetOperationHandle()).compareTo(other.isSetOperationHandle());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetOperationHandle()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.operationHandle, other.operationHandle);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetColumns()).compareTo(other.isSetColumns());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetColumns()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.columns, other.columns);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetOperationType()).compareTo(other.isSetOperationType());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetOperationType()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.operationType, other.operationType);
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
    StringBuilder sb = new StringBuilder("TSExecuteStatementResp(");
    boolean first = true;

    sb.append("status:");
    if (this.status == null) {
      sb.append("null");
    } else {
      sb.append(this.status);
    }
    first = false;
    if (isSetOperationHandle()) {
      if (!first) sb.append(", ");
      sb.append("operationHandle:");
      if (this.operationHandle == null) {
        sb.append("null");
      } else {
        sb.append(this.operationHandle);
      }
      first = false;
    }
    if (isSetColumns()) {
      if (!first) sb.append(", ");
      sb.append("columns:");
      if (this.columns == null) {
        sb.append("null");
      } else {
        sb.append(this.columns);
      }
      first = false;
    }
    if (isSetOperationType()) {
      if (!first) sb.append(", ");
      sb.append("operationType:");
      if (this.operationType == null) {
        sb.append("null");
      } else {
        sb.append(this.operationType);
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
    if (operationHandle != null) {
      operationHandle.validate();
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

  private static class TSExecuteStatementRespStandardSchemeFactory implements SchemeFactory {
    public TSExecuteStatementRespStandardScheme getScheme() {
      return new TSExecuteStatementRespStandardScheme();
    }
  }

  private static class TSExecuteStatementRespStandardScheme extends StandardScheme<TSExecuteStatementResp> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TSExecuteStatementResp struct) throws org.apache.thrift.TException {
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
          case 2: // OPERATION_HANDLE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.operationHandle = new TSOperationHandle();
              struct.operationHandle.read(iprot);
              struct.setOperationHandleIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // COLUMNS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list8 = iprot.readListBegin();
                struct.columns = new ArrayList<String>(_list8.size);
                for (int _i9 = 0; _i9 < _list8.size; ++_i9)
                {
                  String _elem10;
                  _elem10 = iprot.readString();
                  struct.columns.add(_elem10);
                }
                iprot.readListEnd();
              }
              struct.setColumnsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // OPERATION_TYPE
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list11 = iprot.readListBegin();
                struct.operationType = new ArrayList<String>(_list11.size);
                for (int _i12 = 0; _i12 < _list11.size; ++_i12)
                {
                  String _elem13;
                  _elem13 = iprot.readString();
                  struct.operationType.add(_elem13);
                }
                iprot.readListEnd();
              }
              struct.setOperationTypeIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, TSExecuteStatementResp struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.status != null) {
        oprot.writeFieldBegin(STATUS_FIELD_DESC);
        struct.status.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.operationHandle != null) {
        if (struct.isSetOperationHandle()) {
          oprot.writeFieldBegin(OPERATION_HANDLE_FIELD_DESC);
          struct.operationHandle.write(oprot);
          oprot.writeFieldEnd();
        }
      }
      if (struct.columns != null) {
        if (struct.isSetColumns()) {
          oprot.writeFieldBegin(COLUMNS_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.columns.size()));
            for (String _iter14 : struct.columns)
            {
              oprot.writeString(_iter14);
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      if (struct.operationType != null) {
        if (struct.isSetOperationType()) {
          oprot.writeFieldBegin(OPERATION_TYPE_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.operationType.size()));
            for (String _iter15 : struct.operationType)
            {
              oprot.writeString(_iter15);
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TSExecuteStatementRespTupleSchemeFactory implements SchemeFactory {
    public TSExecuteStatementRespTupleScheme getScheme() {
      return new TSExecuteStatementRespTupleScheme();
    }
  }

  private static class TSExecuteStatementRespTupleScheme extends TupleScheme<TSExecuteStatementResp> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TSExecuteStatementResp struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      struct.status.write(oprot);
      BitSet optionals = new BitSet();
      if (struct.isSetOperationHandle()) {
        optionals.set(0);
      }
      if (struct.isSetColumns()) {
        optionals.set(1);
      }
      if (struct.isSetOperationType()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetOperationHandle()) {
        struct.operationHandle.write(oprot);
      }
      if (struct.isSetColumns()) {
        {
          oprot.writeI32(struct.columns.size());
          for (String _iter16 : struct.columns)
          {
            oprot.writeString(_iter16);
          }
        }
      }
      if (struct.isSetOperationType()) {
        {
          oprot.writeI32(struct.operationType.size());
          for (String _iter17 : struct.operationType)
          {
            oprot.writeString(_iter17);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TSExecuteStatementResp struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.status = new TS_Status();
      struct.status.read(iprot);
      struct.setStatusIsSet(true);
      BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.operationHandle = new TSOperationHandle();
        struct.operationHandle.read(iprot);
        struct.setOperationHandleIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TList _list18 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.columns = new ArrayList<String>(_list18.size);
          for (int _i19 = 0; _i19 < _list18.size; ++_i19)
          {
            String _elem20;
            _elem20 = iprot.readString();
            struct.columns.add(_elem20);
          }
        }
        struct.setColumnsIsSet(true);
      }
      if (incoming.get(2)) {
        {
          org.apache.thrift.protocol.TList _list21 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.operationType = new ArrayList<String>(_list21.size);
          for (int _i22 = 0; _i22 < _list21.size; ++_i22)
          {
            String _elem23;
            _elem23 = iprot.readString();
            struct.operationType.add(_elem23);
          }
        }
        struct.setOperationTypeIsSet(true);
      }
    }
  }

}

