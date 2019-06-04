package org.apache.iotdb.db.test;

import org.apache.iotdb.db.conf.directories.Directories;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class Test1 {

  private Directories directories = Directories.getInstance();

  @Before
  public void setup() {
    System.out.println("Test1.setup");
  }

  @After
  public void teardown() {
    System.out.println("Test1.teardown");
  }

  @Test
  public void method1() {
    System.out.println("Test1.method1");
  }

  @Test
  public void method2() {
    System.out.println("Test1.method2");
  }

  @Test
  public void method3() {
    System.out.println("Test1.method3");
  }
}
