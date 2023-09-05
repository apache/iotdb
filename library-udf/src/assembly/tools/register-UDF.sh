#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#


# Parameters
host=127.0.0.1
rpcPort=6667
user=root
pass=root

# Data Profiling

../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function distinct as 'org.apache.iotdb.library.dprofile.UDTFDistinct'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function histogram as 'org.apache.iotdb.library.dprofile.UDTFHistogram'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function integral as 'org.apache.iotdb.library.dprofile.UDAFIntegral'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function integralavg as 'org.apache.iotdb.library.dprofile.UDAFIntegralAvg'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function mad as 'org.apache.iotdb.library.dprofile.UDAFMad'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function median as 'org.apache.iotdb.library.dprofile.UDAFMedian'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function percentile as 'org.apache.iotdb.library.dprofile.UDAFPercentile'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function quantile as 'org.apache.iotdb.library.dprofile.UDAFQuantile'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function period as 'org.apache.iotdb.library.dprofile.UDAFPeriod'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function qlb as 'org.apache.iotdb.library.dprofile.UDTFQLB'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function re_sample as 'org.apache.iotdb.library.dprofile.UDTFResample'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function sample as 'org.apache.iotdb.library.dprofile.UDTFSample'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function segment as 'org.apache.iotdb.library.dprofile.UDTFSegment'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function skew as 'org.apache.iotdb.library.dprofile.UDAFSkew'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function spread as 'org.apache.iotdb.library.dprofile.UDAFSpread'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function stddev as 'org.apache.iotdb.library.dprofile.UDAFStddev'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function minmax as 'org.apache.iotdb.library.dprofile.UDTFMinMax'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function zscore as 'org.apache.iotdb.library.dprofile.UDTFZScore'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function spline as 'org.apache.iotdb.library.dprofile.UDTFSpline'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function mvavg as 'org.apache.iotdb.library.dprofile.UDTFMvAvg'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function acf as 'org.apache.iotdb.library.dprofile.UDTFACF'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function pacf as 'org.apache.iotdb.library.dprofile.UDTFPACF'"


# Data Quality
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function completeness as 'org.apache.iotdb.library.dquality.UDTFCompleteness'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function consistency as 'org.apache.iotdb.library.dquality.UDTFConsistency'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function timeliness as 'org.apache.iotdb.library.dquality.UDTFTimeliness'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function validity as 'org.apache.iotdb.library.dquality.UDTFValidity'"


# Data Repairing
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function timestamprepair as 'org.apache.iotdb.library.drepair.UDTFTimestampRepair'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function valuerepair as 'org.apache.iotdb.library.drepair.UDTFValueRepair'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function valuefill as 'org.apache.iotdb.library.drepair.UDTFValueFill'"


# Data Matching
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function cov as 'org.apache.iotdb.library.dmatch.UDAFCov'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function xcorr as 'org.apache.iotdb.library.dmatch.UDTFXCorr'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function dtw as 'org.apache.iotdb.library.dmatch.UDAFDtw'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function ptnsym as 'org.apache.iotdb.library.dmatch.UDTFPtnSym'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function pearson as 'org.apache.iotdb.library.dmatch.UDAFPearson'"


# Anomaly Detection
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function ksigma as 'org.apache.iotdb.library.anomaly.UDTFKSigma'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function LOF as 'org.apache.iotdb.library.anomaly.UDTFLOF'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function range as 'org.apache.iotdb.library.anomaly.UDTFRange'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function twosidedfilter as 'org.apache.iotdb.library.anomaly.UDTFTwoSidedFilter'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function iqr as 'org.apache.iotdb.library.anomaly.UDTFIQR'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function missdetect as 'org.apache.iotdb.library.anomaly.UDTFMissDetect'"


# Frequency Domain
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function fft as 'org.apache.iotdb.library.frequency.UDTFFFT'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function conv as 'org.apache.iotdb.library.frequency.UDTFConv'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function deconv as 'org.apache.iotdb.library.frequency.UDTFDeconv'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function lowpass as 'org.apache.iotdb.library.frequency.UDTFLowPass'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function highpass as 'org.apache.iotdb.library.frequency.UDTFHighPass'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function dwt as 'org.apache.iotdb.library.frequency.UDTFDWT'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function idwt as 'org.apache.iotdb.library.frequency.UDTFIDWT'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function ifft as 'org.apache.iotdb.library.frequency.UDTFIFFT'"

# Series Discovery
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function consecutivesequences as 'org.apache.iotdb.library.series.UDTFConsecutiveSequences'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function consecutivewindows as 'org.apache.iotdb.library.series.UDTFConsecutiveWindows'"

# String Processing
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function regexsplit as 'org.apache.iotdb.library.string.UDTFRegexSplit'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function regexmatch as 'org.apache.iotdb.library.string.UDTFRegexMatch'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function strreplace as 'org.apache.iotdb.library.string.UDTFStrReplace'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function regexreplace as 'org.apache.iotdb.library.string.UDTFRegexReplace'"

