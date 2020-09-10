/*
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
package org.apache.iotdb.db.http.core;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.CharsetUtil;
import org.apache.iotdb.db.http.constant.HttpConstant;
import org.apache.iotdb.db.http.router.Router;

public class HttpSnoopServerHandler extends SimpleChannelInboundHandler<Object> {

  private FullHttpRequest request;

  private Router router = new Router();

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
    HttpResponseStatus status;
    if (msg instanceof HttpRequest) {
      FullHttpRequest request = this.request = (FullHttpRequest) msg;
      JSON result;
      try {
        result = router.route(request.method(), request.uri(),
            JSON.parseObject(request.content().toString(CharsetUtil.UTF_8)));
        status = OK;
      } catch (Exception e) {
        result = new JSONObject();
        ((JSONObject) result).put(HttpConstant.ERROR, e.getMessage());
        status = INTERNAL_SERVER_ERROR;
      }

      writeResponse(request, ctx, result, status);
    }
  }

  private void writeResponse(HttpObject currentObj, ChannelHandlerContext ctx, JSON json, HttpResponseStatus status) {

    // Decide whether to close the connection or not.
    boolean keepAlive = HttpUtil.isKeepAlive(request);
    // Build the response object.
    FullHttpResponse response = new DefaultFullHttpResponse(
        HTTP_1_1, status,
        Unpooled.copiedBuffer(json.toString(), CharsetUtil.UTF_8));

    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8");



    if (keepAlive) {
      // Add 'Content-Length' header only for a keep-alive connection.
      response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
      // Add keep alive header as per:
      // - http://www.w3.org/Protocols/HTTP/1.1/draft-ietf-http-v11-spec-01.html#Connection
      response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
    }

    // Write the response.
    ctx.write(response);

  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    cause.printStackTrace();
    ctx.close();
  }
}
