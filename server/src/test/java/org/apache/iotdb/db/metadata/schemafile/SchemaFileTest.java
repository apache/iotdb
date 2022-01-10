package org.apache.iotdb.db.metadata.schemafile;

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOError;
import java.io.IOException;

public class SchemaFileTest {

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void essentialTest() {
  }

  @Test
  public void initSchemaFile() throws IOException {
    ISchemaFile sf = new SchemaFile("tsg4");
    sf.close();
  }

  /**
   * Steps inside SchemaFile:
   * 1. check storage group name with files
   * 2.
   * */
  @Test
  public void storeTree() {
    int x = 1;
    System.out.println(++x);
  }
}
