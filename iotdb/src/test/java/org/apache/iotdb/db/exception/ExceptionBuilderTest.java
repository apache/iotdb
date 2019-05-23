/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.exception;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Properties;
import org.apache.iotdb.db.exception.builder.ExceptionBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExceptionBuilderTest {

  @Before
  public void before() {
    Properties prop = new Properties();
    FileOutputStream out1 = null;
    FileOutputStream out2 = null;
    try {
      out1 = new FileOutputStream("err_info_en.properties", true);
      prop.setProperty("20000", "Unknown error");
      prop.setProperty("20001", "No parameters exist in the statement");
      prop.setProperty("20002", "Invalid parameter number");
      prop.setProperty("20003", "Can't connect to server on {}({})");
      prop.setProperty("20061", "Authentication plugin {} reported error: {}");
      prop.setProperty("20062", "Insecure API function call: {}");
      prop.setProperty("20064", "Client ran out of memory");
      prop.setProperty("20130", "Statement not prepared");
      prop.setProperty("20220", "Fail to connect");

      prop.store(new OutputStreamWriter(out1, "utf-8"), "english version");

      out2 = new FileOutputStream("err_info_cn.properties", true);
      prop.setProperty("20000", "未知错误");
      prop.setProperty("20001", "语句中无变量");
      prop.setProperty("20002", "无效的变量");
      prop.setProperty("20003", "无法连接到服务器");
      prop.setProperty("20061", "验证失败");
      prop.setProperty("20062", "不安全的函数调用");
      prop.setProperty("20064", "客户端内存溢出");
      prop.setProperty("20130", "语句未就绪");
      prop.setProperty("20220", "连接失败");

      prop.store(new OutputStreamWriter(out2, "utf-8"), "chinese version");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        out1.close();
        out2.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @After
  public void after() {
    File file1 = new File("err_info_en.properties");
    File file2 = new File("err_info_cn.properties");
    if (file1.exists()) {
      file1.delete();
    }
    if (file2.exists()) {
      file2.delete();
    }
  }

  @Test
  public void testLoadProp() {
    ExceptionBuilder excpHandler = new ExceptionBuilder();
    excpHandler.loadInfo("err_info_en.properties");
    assertEquals("Invalid parameter number", excpHandler.searchInfo(20002));
    assertEquals("Can't connect to server on {}({})", excpHandler.searchInfo(20003));

    excpHandler.loadInfo("err_info_cn.properties");
    assertEquals("无法连接到服务器", excpHandler.searchInfo(20003));
    assertEquals("验证失败", excpHandler.searchInfo(20061));
  }

}
