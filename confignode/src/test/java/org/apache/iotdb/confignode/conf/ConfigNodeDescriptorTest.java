package org.apache.iotdb.confignode.conf;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;

public class ConfigNodeDescriptorTest {

  private final String confPath = System.getProperty(ConfigNodeConf.CONF_NAME, null);

  @Before
  public void init() {
    org.apache.catalina.webresources.TomcatURLStreamHandlerFactory.getInstance();
  }

  @After
  public void clear() {
    if (confPath != null) {
      System.setProperty(ConfigNodeConstant.CONFIG_NODE_CONF, confPath);
    } else {
      System.clearProperty(ConfigNodeConstant.CONFIG_NODE_CONF);
    }
  }

  @Test
  public void testConfigURLWithFileProtocol() {
    ConfigNodeDescriptor desc = ConfigNodeDescriptor.getInstance();
    String pathString = "file:/usr/local/bin";

    System.setProperty(ConfigNodeConstant.CONFIG_NODE_CONF, pathString);
    URL confURL = desc.getPropsUrl();
    Assert.assertTrue(confURL.toString().startsWith(pathString));
  }

  @Test
  public void testConfigURLWithClasspathProtocol() {
    ConfigNodeDescriptor desc = ConfigNodeDescriptor.getInstance();

    String pathString = "classpath:/root/path";
    System.setProperty(ConfigNodeConstant.CONFIG_NODE_CONF, pathString);
    URL confURL = desc.getPropsUrl();
    Assert.assertTrue(confURL.toString().startsWith(pathString));
  }

  @Test
  public void testConfigURLWithPlainFilePath() {
    ConfigNodeDescriptor desc = ConfigNodeDescriptor.getInstance();
    URL path = ConfigNodeConf.class.getResource("/" + ConfigNodeConf.CONF_NAME);
    // filePath is a plain path string
    String filePath = path.getFile();
    System.setProperty(ConfigNodeConstant.CONFIG_NODE_CONF, filePath);
    URL confURL = desc.getPropsUrl();
    Assert.assertEquals(confURL.toString(), path.toString());
  }
}
