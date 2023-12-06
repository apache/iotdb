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
package org.apache.iotdb.commons.utils;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.exception.BadNodeUrlException;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class NodeUrlUtilsTest {

  @Test
  public void parseAndConvertTEndPointUrlsTest() throws BadNodeUrlException {
    final List<TEndPoint> endPoints =
        Arrays.asList(
            new TEndPoint("0.0.0.0", 6667),
            new TEndPoint("0.0.0.0", 6668),
            new TEndPoint("0.0.0.0", 6669));
    final String endPointUrls = "0.0.0.0:6667,0.0.0.0:6668,0.0.0.0:6669";

    Assert.assertEquals(endPointUrls, NodeUrlUtils.convertTEndPointUrls(endPoints));
    Assert.assertEquals(endPoints, NodeUrlUtils.parseTEndPointUrls(endPointUrls));
  }

  @Test
  public void parseAndConvertTConfigNodeUrlsTest() throws BadNodeUrlException {
    final List<TConfigNodeLocation> configNodeLocations =
        Arrays.asList(
            new TConfigNodeLocation(
                0, new TEndPoint("0.0.0.0", 10710), new TEndPoint("0.0.0.0", 10720)),
            new TConfigNodeLocation(
                1, new TEndPoint("0.0.0.0", 10711), new TEndPoint("0.0.0.0", 10721)),
            new TConfigNodeLocation(
                2, new TEndPoint("0.0.0.0", 10712), new TEndPoint("0.0.0.0", 10722)));
    final String configNodeUrls =
        "0,0.0.0.0:10710,0.0.0.0:10720;1,0.0.0.0:10711,0.0.0.0:10721;2,0.0.0.0:10712,0.0.0.0:10722";

    Assert.assertEquals(configNodeUrls, NodeUrlUtils.convertTConfigNodeUrls(configNodeLocations));
    Assert.assertEquals(configNodeLocations, NodeUrlUtils.parseTConfigNodeUrls(configNodeUrls));
  }

  @Test
  public void parseAndConvertTEndPointUrlsIPV4AndIPV6Test() throws BadNodeUrlException {
    final List<TEndPoint> endPoints =
        Arrays.asList(
            new TEndPoint("AD80:E32B:CA25:B3AE:DC4C:DAAF:CCDE:2345", 6667),
            new TEndPoint("0:0:0:0:0:FFFF:129.144.52.38", 6668),
            new TEndPoint("::13.1.68.3", 6669));
    final String endPointUrls =
        "AD80:E32B:CA25:B3AE:DC4C:DAAF:CCDE:2345:6667,[0:0:0:0:0:FFFF:129.144.52.38]:6668,[::13.1.68.3]:6669";
    Assert.assertEquals(endPoints, NodeUrlUtils.parseTEndPointUrls(endPointUrls));
  }

  @Test
  public void parseAndConvertTConfigNodeUrlsIPV4AndIPV6Test() throws BadNodeUrlException {
    final List<TConfigNodeLocation> configNodeLocations =
        Arrays.asList(
            new TConfigNodeLocation(
                0,
                new TEndPoint("AD80:E32B:CA25:B3AE:DC4C:DAAF:CDDE:ABFD", 22277),
                new TEndPoint("AD80:E32B:CA25:B3AE:DC4C:DAAF:CDDE:ABFD", 22278)),
            new TConfigNodeLocation(
                1,
                new TEndPoint("AD80:E32B:CA25:B3AE:DC4C:DAAF:CDDE:ABFD", 22279),
                new TEndPoint("AD80:E32B:CA25:B3AE:DC4C:DAAF:CDDE:ABFD", 22280)),
            new TConfigNodeLocation(
                2,
                new TEndPoint("AD80:E32B:CA25:B3AE:DC4C:DAAF:CDDE:ABFD", 22281),
                new TEndPoint("AD80:E32B:CA25:B3AE:DC4C:DAAF:CDDE:ABFD", 22282)));
    final String configNodeUrls =
        "0,AD80:E32B:CA25:B3AE:DC4C:DAAF:CDDE:ABFD:22277,[AD80:E32B:CA25:B3AE:DC4C:DAAF:CDDE:ABFD]:22278;1,AD80:E32B:CA25:B3AE:DC4C:DAAF:CDDE:ABFD:22279,AD80:E32B:CA25:B3AE:DC4C:DAAF:CDDE:ABFD:22280;2,AD80:E32B:CA25:B3AE:DC4C:DAAF:CDDE:ABFD:22281,AD80:E32B:CA25:B3AE:DC4C:DAAF:CDDE:ABFD:22282";
    Assert.assertEquals(configNodeLocations, NodeUrlUtils.parseTConfigNodeUrls(configNodeUrls));
  }
}
