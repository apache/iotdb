package org.apache.iotdb.db.auth.user;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.auth.user.LocalFileUserManager;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LocalFileUserManagerTest {

  private File testFolder;
  private LocalFileUserManager manager;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    testFolder = new File(TestConstant.BASE_OUTPUT_PATH.concat("test"));
    testFolder.mkdirs();
    manager = new LocalFileUserManager(testFolder.getPath());
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(testFolder);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testIllegalInput() throws AuthException {
    // Password contains space
    try {
      manager.createEntry("username1", "password_ ", true);
    } catch (AuthException e) {
      assertTrue(e.getMessage().contains("cannot contain spaces"));
    }
    // Username contains space
    try {
      assertFalse(manager.createEntry("username 2", "password_", true));
    } catch (AuthException e) {
      assertTrue(e.getMessage().contains("cannot contain spaces"));
    }
  }

  @Test
  public void testCreateUserRawPassword() throws AuthException {
    Assert.assertTrue(
        manager.createEntry("testRaw", AuthUtils.encryptPassword("password1"), true, false));
    User user = manager.getEntry("testRaw");
    Assert.assertEquals(user.getPassword(), AuthUtils.encryptPassword("password1"));
  }
}
