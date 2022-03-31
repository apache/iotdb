package org.apache.iotdb.confignode.auth;

import org.apache.iotdb.confignode.rpc.thrift.ConfigIService;
import org.apache.iotdb.confignode.rpc.thrift.CreateUserReq;
import org.apache.iotdb.confignode.utils.ConfigNodeEnvironmentUtils;
import org.apache.iotdb.db.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AuthorTest {

  IAuthorizer authorizer;

  @Before
  public void setUp() throws Exception {
    ConfigNodeEnvironmentUtils.envSetUp();
    authorizer = BasicAuthorizer.getInstance();
  }

  @After
  public void tearDown() throws Exception {
    ConfigNodeEnvironmentUtils.cleanEnv();
  }

  @Test
  public void createUserTest() throws TException {
    ConfigIService.Client client;
    TTransport transport = null;
    transport = RpcTransportFactory.INSTANCE.getTransport("0.0.0.0", 22277, 2000);
    transport.open();
    client = new ConfigIService.Client(new TBinaryProtocol(transport));
    TSStatus tsStatus = client.createUser(new CreateUserReq("root1", "root1"));
    System.out.println(tsStatus.getCode());
    System.out.println(tsStatus.getMessage());
    Assert.assertEquals(tsStatus.getCode(), TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }
}
