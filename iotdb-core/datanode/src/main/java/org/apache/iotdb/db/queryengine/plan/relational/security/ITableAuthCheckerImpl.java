package org.apache.iotdb.db.queryengine.plan.relational.security;

import org.apache.iotdb.commons.auth.AuthException;

public class ITableAuthCheckerImpl extends ITableAuthChecker {

  void checkDatabaseVisibility(String userName, String databaseName) throws AuthException {}
}
