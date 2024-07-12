package org.apache.iotdb.commons.auth.entity;

import org.apache.iotdb.commons.path.PartialPath;

public class PrivilegeUnin {
  private String dbname;
  private String tbname;
  private PartialPath path;

  private boolean grantOption;
  private final PrivilegeType privilegeType;

  private final PrivilegeModelType modelType;

  public PrivilegeUnin(String dbname, PrivilegeType type, boolean grantOption) {
    assert (type.isObjectPrivilege());
    this.dbname = dbname;
    this.privilegeType = type;
    this.modelType = PrivilegeModelType.RELATIONAL;
    this.grantOption = grantOption;
  }

  public PrivilegeUnin(String dbname, PrivilegeType type) {
    assert (type.isObjectPrivilege());
    this.dbname = dbname;
    this.privilegeType = type;
    this.modelType = PrivilegeModelType.RELATIONAL;
  }

  public PrivilegeUnin(String dbname, String tbname, PrivilegeType type, boolean grantOption) {
    assert (type.isObjectPrivilege());
    this.dbname = dbname;
    this.tbname = tbname;
    this.privilegeType = type;
    this.modelType = PrivilegeModelType.RELATIONAL;
    this.grantOption = grantOption;
  }

  public PrivilegeUnin(String dbname, String tbname, PrivilegeType type) {
    assert (type.isObjectPrivilege());
    this.dbname = dbname;
    this.tbname = tbname;
    this.privilegeType = type;
    this.modelType = PrivilegeModelType.RELATIONAL;
  }

  public PrivilegeUnin(PartialPath path, PrivilegeType type, boolean grantOption) {
    assert (type.isPathPrivilege());
    this.path = path;
    this.privilegeType = type;
    this.modelType = PrivilegeModelType.TREE;
    this.grantOption = grantOption;
  }

  public PrivilegeUnin(PartialPath path, PrivilegeType type) {
    assert (type.isPathPrivilege());
    this.path = path;
    this.privilegeType = type;
    this.modelType = PrivilegeModelType.TREE;
    this.grantOption = false;
  }

  public PrivilegeUnin(PrivilegeType type, boolean grantOption) {
    assert (type.isSystemPrivilege());
    this.privilegeType = type;
    this.modelType = PrivilegeModelType.SYSTEM;
    this.grantOption = grantOption;
  }

  public PrivilegeUnin(PrivilegeType type) {
    assert (type.isSystemPrivilege());
    this.privilegeType = type;
    this.modelType = PrivilegeModelType.SYSTEM;
    this.grantOption = false;
  }

  public PrivilegeModelType ModelType() {
    return this.modelType;
  }

  public String getDBName() {
    return this.dbname;
  }

  public String getTbName() {
    return this.tbname;
  }

  public PartialPath getPath() {
    return this.path;
  }

  public PrivilegeType getPrivilegeType() {
    return this.privilegeType;
  }

  public boolean isGrantOption() {
    return this.grantOption;
  }
}
