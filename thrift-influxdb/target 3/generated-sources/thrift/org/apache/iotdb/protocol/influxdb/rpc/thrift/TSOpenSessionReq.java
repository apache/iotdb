/**
 * Autogenerated by Thrift Compiler (0.14.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.iotdb.protocol.influxdb.rpc.thrift;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.14.1)", date = "2022-01-28")
public class TSOpenSessionReq implements org.apache.thrift.TBase<TSOpenSessionReq, TSOpenSessionReq._Fields>, java.io.Serializable, Cloneable, Comparable<TSOpenSessionReq> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TSOpenSessionReq");

  private static final org.apache.thrift.protocol.TField ZONE_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("zoneId", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField USERNAME_FIELD_DESC = new org.apache.thrift.protocol.TField("username", org.apache.thrift.protocol.TType.STRING, (short)3);
  private static final org.apache.thrift.protocol.TField PASSWORD_FIELD_DESC = new org.apache.thrift.protocol.TField("password", org.apache.thrift.protocol.TType.STRING, (short)4);
  private static final org.apache.thrift.protocol.TField CONFIGURATION_FIELD_DESC = new org.apache.thrift.protocol.TField("configuration", org.apache.thrift.protocol.TType.MAP, (short)5);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new TSOpenSessionReqStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new TSOpenSessionReqTupleSchemeFactory();

  public @org.apache.thrift.annotation.Nullable java.lang.String zoneId; // required
  public @org.apache.thrift.annotation.Nullable java.lang.String username; // optional
  public @org.apache.thrift.annotation.Nullable java.lang.String password; // optional
  public @org.apache.thrift.annotation.Nullable java.util.Map<java.lang.String,java.lang.String> configuration; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    ZONE_ID((short)2, "zoneId"),
    USERNAME((short)3, "username"),
    PASSWORD((short)4, "password"),
    CONFIGURATION((short)5, "configuration");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 2: // ZONE_ID
          return ZONE_ID;
        case 3: // USERNAME
          return USERNAME;
        case 4: // PASSWORD
          return PASSWORD;
        case 5: // CONFIGURATION
          return CONFIGURATION;
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
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final _Fields optionals[] = {_Fields.USERNAME,_Fields.PASSWORD,_Fields.CONFIGURATION};
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.ZONE_ID, new org.apache.thrift.meta_data.FieldMetaData("zoneId", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.USERNAME, new org.apache.thrift.meta_data.FieldMetaData("username", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.PASSWORD, new org.apache.thrift.meta_data.FieldMetaData("password", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.CONFIGURATION, new org.apache.thrift.meta_data.FieldMetaData("configuration", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TSOpenSessionReq.class, metaDataMap);
  }

  public TSOpenSessionReq() {
  }

  public TSOpenSessionReq(
    java.lang.String zoneId)
  {
    this();
    this.zoneId = zoneId;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TSOpenSessionReq(TSOpenSessionReq other) {
    if (other.isSetZoneId()) {
      this.zoneId = other.zoneId;
    }
    if (other.isSetUsername()) {
      this.username = other.username;
    }
    if (other.isSetPassword()) {
      this.password = other.password;
    }
    if (other.isSetConfiguration()) {
      java.util.Map<java.lang.String,java.lang.String> __this__configuration = new java.util.HashMap<java.lang.String,java.lang.String>(other.configuration);
      this.configuration = __this__configuration;
    }
  }

  public TSOpenSessionReq deepCopy() {
    return new TSOpenSessionReq(this);
  }

  @Override
  public void clear() {
    this.zoneId = null;
    this.username = null;
    this.password = null;
    this.configuration = null;
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getZoneId() {
    return this.zoneId;
  }

  public TSOpenSessionReq setZoneId(@org.apache.thrift.annotation.Nullable java.lang.String zoneId) {
    this.zoneId = zoneId;
    return this;
  }

  public void unsetZoneId() {
    this.zoneId = null;
  }

  /** Returns true if field zoneId is set (has been assigned a value) and false otherwise */
  public boolean isSetZoneId() {
    return this.zoneId != null;
  }

  public void setZoneIdIsSet(boolean value) {
    if (!value) {
      this.zoneId = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getUsername() {
    return this.username;
  }

  public TSOpenSessionReq setUsername(@org.apache.thrift.annotation.Nullable java.lang.String username) {
    this.username = username;
    return this;
  }

  public void unsetUsername() {
    this.username = null;
  }

  /** Returns true if field username is set (has been assigned a value) and false otherwise */
  public boolean isSetUsername() {
    return this.username != null;
  }

  public void setUsernameIsSet(boolean value) {
    if (!value) {
      this.username = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getPassword() {
    return this.password;
  }

  public TSOpenSessionReq setPassword(@org.apache.thrift.annotation.Nullable java.lang.String password) {
    this.password = password;
    return this;
  }

  public void unsetPassword() {
    this.password = null;
  }

  /** Returns true if field password is set (has been assigned a value) and false otherwise */
  public boolean isSetPassword() {
    return this.password != null;
  }

  public void setPasswordIsSet(boolean value) {
    if (!value) {
      this.password = null;
    }
  }

  public int getConfigurationSize() {
    return (this.configuration == null) ? 0 : this.configuration.size();
  }

  public void putToConfiguration(java.lang.String key, java.lang.String val) {
    if (this.configuration == null) {
      this.configuration = new java.util.HashMap<java.lang.String,java.lang.String>();
    }
    this.configuration.put(key, val);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Map<java.lang.String,java.lang.String> getConfiguration() {
    return this.configuration;
  }

  public TSOpenSessionReq setConfiguration(@org.apache.thrift.annotation.Nullable java.util.Map<java.lang.String,java.lang.String> configuration) {
    this.configuration = configuration;
    return this;
  }

  public void unsetConfiguration() {
    this.configuration = null;
  }

  /** Returns true if field configuration is set (has been assigned a value) and false otherwise */
  public boolean isSetConfiguration() {
    return this.configuration != null;
  }

  public void setConfigurationIsSet(boolean value) {
    if (!value) {
      this.configuration = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case ZONE_ID:
      if (value == null) {
        unsetZoneId();
      } else {
        setZoneId((java.lang.String)value);
      }
      break;

    case USERNAME:
      if (value == null) {
        unsetUsername();
      } else {
        setUsername((java.lang.String)value);
      }
      break;

    case PASSWORD:
      if (value == null) {
        unsetPassword();
      } else {
        setPassword((java.lang.String)value);
      }
      break;

    case CONFIGURATION:
      if (value == null) {
        unsetConfiguration();
      } else {
        setConfiguration((java.util.Map<java.lang.String,java.lang.String>)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case ZONE_ID:
      return getZoneId();

    case USERNAME:
      return getUsername();

    case PASSWORD:
      return getPassword();

    case CONFIGURATION:
      return getConfiguration();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case ZONE_ID:
      return isSetZoneId();
    case USERNAME:
      return isSetUsername();
    case PASSWORD:
      return isSetPassword();
    case CONFIGURATION:
      return isSetConfiguration();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof TSOpenSessionReq)
      return this.equals((TSOpenSessionReq)that);
    return false;
  }

  public boolean equals(TSOpenSessionReq that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_zoneId = true && this.isSetZoneId();
    boolean that_present_zoneId = true && that.isSetZoneId();
    if (this_present_zoneId || that_present_zoneId) {
      if (!(this_present_zoneId && that_present_zoneId))
        return false;
      if (!this.zoneId.equals(that.zoneId))
        return false;
    }

    boolean this_present_username = true && this.isSetUsername();
    boolean that_present_username = true && that.isSetUsername();
    if (this_present_username || that_present_username) {
      if (!(this_present_username && that_present_username))
        return false;
      if (!this.username.equals(that.username))
        return false;
    }

    boolean this_present_password = true && this.isSetPassword();
    boolean that_present_password = true && that.isSetPassword();
    if (this_present_password || that_present_password) {
      if (!(this_present_password && that_present_password))
        return false;
      if (!this.password.equals(that.password))
        return false;
    }

    boolean this_present_configuration = true && this.isSetConfiguration();
    boolean that_present_configuration = true && that.isSetConfiguration();
    if (this_present_configuration || that_present_configuration) {
      if (!(this_present_configuration && that_present_configuration))
        return false;
      if (!this.configuration.equals(that.configuration))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetZoneId()) ? 131071 : 524287);
    if (isSetZoneId())
      hashCode = hashCode * 8191 + zoneId.hashCode();

    hashCode = hashCode * 8191 + ((isSetUsername()) ? 131071 : 524287);
    if (isSetUsername())
      hashCode = hashCode * 8191 + username.hashCode();

    hashCode = hashCode * 8191 + ((isSetPassword()) ? 131071 : 524287);
    if (isSetPassword())
      hashCode = hashCode * 8191 + password.hashCode();

    hashCode = hashCode * 8191 + ((isSetConfiguration()) ? 131071 : 524287);
    if (isSetConfiguration())
      hashCode = hashCode * 8191 + configuration.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(TSOpenSessionReq other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetZoneId(), other.isSetZoneId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetZoneId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.zoneId, other.zoneId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetUsername(), other.isSetUsername());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetUsername()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.username, other.username);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetPassword(), other.isSetPassword());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPassword()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.password, other.password);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetConfiguration(), other.isSetConfiguration());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetConfiguration()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.configuration, other.configuration);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("TSOpenSessionReq(");
    boolean first = true;

    sb.append("zoneId:");
    if (this.zoneId == null) {
      sb.append("null");
    } else {
      sb.append(this.zoneId);
    }
    first = false;
    if (isSetUsername()) {
      if (!first) sb.append(", ");
      sb.append("username:");
      if (this.username == null) {
        sb.append("null");
      } else {
        sb.append(this.username);
      }
      first = false;
    }
    if (isSetPassword()) {
      if (!first) sb.append(", ");
      sb.append("password:");
      if (this.password == null) {
        sb.append("null");
      } else {
        sb.append(this.password);
      }
      first = false;
    }
    if (isSetConfiguration()) {
      if (!first) sb.append(", ");
      sb.append("configuration:");
      if (this.configuration == null) {
        sb.append("null");
      } else {
        sb.append(this.configuration);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (zoneId == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'zoneId' was not present! Struct: " + toString());
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

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TSOpenSessionReqStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public TSOpenSessionReqStandardScheme getScheme() {
      return new TSOpenSessionReqStandardScheme();
    }
  }

  private static class TSOpenSessionReqStandardScheme extends org.apache.thrift.scheme.StandardScheme<TSOpenSessionReq> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TSOpenSessionReq struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 2: // ZONE_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.zoneId = iprot.readString();
              struct.setZoneIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // USERNAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.username = iprot.readString();
              struct.setUsernameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // PASSWORD
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.password = iprot.readString();
              struct.setPasswordIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // CONFIGURATION
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map18 = iprot.readMapBegin();
                struct.configuration = new java.util.HashMap<java.lang.String,java.lang.String>(2*_map18.size);
                @org.apache.thrift.annotation.Nullable java.lang.String _key19;
                @org.apache.thrift.annotation.Nullable java.lang.String _val20;
                for (int _i21 = 0; _i21 < _map18.size; ++_i21)
                {
                  _key19 = iprot.readString();
                  _val20 = iprot.readString();
                  struct.configuration.put(_key19, _val20);
                }
                iprot.readMapEnd();
              }
              struct.setConfigurationIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, TSOpenSessionReq struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.zoneId != null) {
        oprot.writeFieldBegin(ZONE_ID_FIELD_DESC);
        oprot.writeString(struct.zoneId);
        oprot.writeFieldEnd();
      }
      if (struct.username != null) {
        if (struct.isSetUsername()) {
          oprot.writeFieldBegin(USERNAME_FIELD_DESC);
          oprot.writeString(struct.username);
          oprot.writeFieldEnd();
        }
      }
      if (struct.password != null) {
        if (struct.isSetPassword()) {
          oprot.writeFieldBegin(PASSWORD_FIELD_DESC);
          oprot.writeString(struct.password);
          oprot.writeFieldEnd();
        }
      }
      if (struct.configuration != null) {
        if (struct.isSetConfiguration()) {
          oprot.writeFieldBegin(CONFIGURATION_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, struct.configuration.size()));
            for (java.util.Map.Entry<java.lang.String, java.lang.String> _iter22 : struct.configuration.entrySet())
            {
              oprot.writeString(_iter22.getKey());
              oprot.writeString(_iter22.getValue());
            }
            oprot.writeMapEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TSOpenSessionReqTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public TSOpenSessionReqTupleScheme getScheme() {
      return new TSOpenSessionReqTupleScheme();
    }
  }

  private static class TSOpenSessionReqTupleScheme extends org.apache.thrift.scheme.TupleScheme<TSOpenSessionReq> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TSOpenSessionReq struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      oprot.writeString(struct.zoneId);
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetUsername()) {
        optionals.set(0);
      }
      if (struct.isSetPassword()) {
        optionals.set(1);
      }
      if (struct.isSetConfiguration()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetUsername()) {
        oprot.writeString(struct.username);
      }
      if (struct.isSetPassword()) {
        oprot.writeString(struct.password);
      }
      if (struct.isSetConfiguration()) {
        {
          oprot.writeI32(struct.configuration.size());
          for (java.util.Map.Entry<java.lang.String, java.lang.String> _iter23 : struct.configuration.entrySet())
          {
            oprot.writeString(_iter23.getKey());
            oprot.writeString(_iter23.getValue());
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TSOpenSessionReq struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.zoneId = iprot.readString();
      struct.setZoneIdIsSet(true);
      java.util.BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.username = iprot.readString();
        struct.setUsernameIsSet(true);
      }
      if (incoming.get(1)) {
        struct.password = iprot.readString();
        struct.setPasswordIsSet(true);
      }
      if (incoming.get(2)) {
        {
          org.apache.thrift.protocol.TMap _map24 = iprot.readMapBegin(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING); 
          struct.configuration = new java.util.HashMap<java.lang.String,java.lang.String>(2*_map24.size);
          @org.apache.thrift.annotation.Nullable java.lang.String _key25;
          @org.apache.thrift.annotation.Nullable java.lang.String _val26;
          for (int _i27 = 0; _i27 < _map24.size; ++_i27)
          {
            _key25 = iprot.readString();
            _val26 = iprot.readString();
            struct.configuration.put(_key25, _val26);
          }
        }
        struct.setConfigurationIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

