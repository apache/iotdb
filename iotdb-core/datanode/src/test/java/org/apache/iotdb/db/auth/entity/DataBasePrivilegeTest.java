package org.apache.iotdb.db.auth.entity;

import org.apache.iotdb.commons.auth.entity.DatabasePrivilege;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class DataBasePrivilegeTest {
  @Test
  public void test() {
    DatabasePrivilege dbPrivilege = new DatabasePrivilege("database");
    dbPrivilege.grantDBPrivilege(PrivilegeType.ALTER);
    dbPrivilege.grantDBPrivilege(PrivilegeType.INSERT);
    dbPrivilege.grantDBGrantOption(PrivilegeType.ALTER);
    dbPrivilege.grantTablePrivilege("test", PrivilegeType.SELECT);
    dbPrivilege.grantTablePrivilege("test", PrivilegeType.DELETE);
    dbPrivilege.grantTablePrivilege("test2", PrivilegeType.SELECT);
    dbPrivilege.grantTableGrantOption("test", PrivilegeType.SELECT);
    Assert.assertTrue(dbPrivilege.checkDBPrivilege(PrivilegeType.ALTER));
    Assert.assertFalse(dbPrivilege.checkDBPrivilege(PrivilegeType.DELETE));
    Assert.assertTrue(dbPrivilege.checkDBGrantOption(PrivilegeType.ALTER));
    Assert.assertTrue(dbPrivilege.checkTablePrivilege("test", PrivilegeType.SELECT));
    Assert.assertFalse(dbPrivilege.checkTablePrivilege("test2", PrivilegeType.CREATE));
    Assert.assertTrue(dbPrivilege.checkTableGrantOption("test", PrivilegeType.SELECT));
    DatabasePrivilege dbPrivilege2 = new DatabasePrivilege();
    try {
      ByteBuffer byteBuffer = dbPrivilege.serialize();
      dbPrivilege2.deserialize(byteBuffer);
    } catch (IOException e) {
      Assert.fail();
    }
    Assert.assertEquals(dbPrivilege, dbPrivilege2);
    String toString = dbPrivilege.toString();
    Assert.assertEquals(
        toString,
        "Database(database):{ALTER_with_grant_option,INSERT,;"
            + " Tables: [ test2(SELECT,) test(SELECT_with_grant_option,DELETE,)]}");
    int mask = dbPrivilege.getAllPrivileges();
    dbPrivilege2.revokeTablePrivilege("test", PrivilegeType.SELECT);
    dbPrivilege2.revokeTableGrantOption("test", PrivilegeType.SELECT);
    dbPrivilege2.revokeTablePrivilege("test", PrivilegeType.DELETE);
    Assert.assertEquals(dbPrivilege2.getTablePrivilegeMap().size(), 1);
    dbPrivilege2.revokeTablePrivilege("test2", PrivilegeType.SELECT);
    dbPrivilege2.revokeDBPrivilege(PrivilegeType.INSERT);
    dbPrivilege2.revokeDBGrantOption(PrivilegeType.ALTER);
    Assert.assertEquals(dbPrivilege2.getTablePrivilegeMap().size(), 0);
    Assert.assertFalse(dbPrivilege2.checkTablePrivilege("test", PrivilegeType.SELECT));
    dbPrivilege2.setPrivileges(mask);
    Assert.assertEquals(dbPrivilege.getPrivilegeSet(), dbPrivilege2.getPrivilegeSet());
    Assert.assertEquals(
        dbPrivilege.getPrivilegeGrantOptSet(), dbPrivilege2.getPrivilegeGrantOptSet());
  }
}
