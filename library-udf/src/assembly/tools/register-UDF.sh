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

../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function distinct as 'org.apache.iotdb.quality.dprofile.UDTFDistinct'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function histogram as 'org.apache.iotdb.quality.dprofile.UDTFHistogram'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function integral as 'org.apache.iotdb.quality.dprofile.UDAFIntegral'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function integralavg as 'org.apache.iotdb.quality.dprofile.UDAFIntegralAvg'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function mad as 'org.apache.iotdb.quality.dprofile.UDAFMad'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function median as 'org.apache.iotdb.quality.dprofile.UDAFMedian'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function mode as 'org.apache.iotdb.quality.dprofile.UDAFMode'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function percentile as 'org.apache.iotdb.quality.dprofile.UDAFPercentile'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function period as 'org.apache.iotdb.quality.dprofile.UDAFPeriod'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function qlb as 'org.apache.iotdb.quality.dprofile.UDTFQLB'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function re_sample as 'org.apache.iotdb.quality.dprofile.UDTFResample'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function sample as 'org.apache.iotdb.quality.dprofile.UDTFSample'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function segment as 'org.apache.iotdb.quality.dprofile.UDTFSegment'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function skew as 'org.apache.iotdb.quality.dprofile.UDAFSkew'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function spread as 'org.apache.iotdb.quality.dprofile.UDAFSpread'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function stddev as 'org.apache.iotdb.quality.dprofile.UDAFStddev'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function minmax as 'org.apache.iotdb.quality.dprofile.UDTFMinMax'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function zscore as 'org.apache.iotdb.quality.dprofile.UDTFZScore'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function spline as 'org.apache.iotdb.quality.dprofile.UDTFSpline'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function mvavg as 'org.apache.iotdb.quality.dprofile.UDTFMvAvg'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function acf as 'org.apache.iotdb.quality.dprofile.UDTFACF'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function pacf as 'org.apache.iotdb.quality.dprofile.UDTFPACF'"


# Data Quality
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function completeness as 'org.apache.iotdb.quality.dquality.UDTFCompleteness'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function consistency as 'org.apache.iotdb.quality.dquality.UDTFConsistency'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function timeliness as 'org.apache.iotdb.quality.dquality.UDTFTimeliness'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function validity as 'org.apache.iotdb.quality.dquality.UDTFValidity'"


# Data Repairing
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function timestamprepair as 'org.apache.iotdb.quality.drepair.UDTFTimestampRepair'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function valuerepair as 'org.apache.iotdb.quality.drepair.UDTFValueRepair'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function valuefill as 'org.apache.iotdb.quality.drepair.UDTFValueFill'"


# Data Matching
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function cov as 'org.apache.iotdb.quality.dmatch.UDAFCov'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function xcorr as 'org.apache.iotdb.quality.dmatch.UDTFXCorr'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function dtw as 'org.apache.iotdb.quality.dmatch.UDAFDtw'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function ptnsym as 'org.apache.iotdb.quality.dmatch.UDTFPtnSym'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function pearson as 'org.apache.iotdb.quality.dmatch.UDAFPearson'"


# Anomaly Detection
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function ksigma as 'org.apache.iotdb.quality.anomaly.UDTFKSigma'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function LOF as 'org.apache.iotdb.quality.anomaly.UDTFLOF'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function range as 'org.apache.iotdb.quality.anomaly.UDTFRange'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function twosidedfilter as 'org.apache.iotdb.quality.anomaly.UDTFTwoSidedFilter'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function iqr as 'org.apache.iotdb.quality.anomaly.UDTFIQR'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function missdetect as 'org.apache.iotdb.quality.anomaly.UDTFMissDetect'"


# Frequency Domain
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function fft as 'org.apache.iotdb.quality.frequency.UDTFFFT'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function stft as 'org.apache.iotdb.quality.frequency.UDTFSTFT'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function conv as 'org.apache.iotdb.quality.frequency.UDTFConv'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function deconv as 'org.apache.iotdb.quality.frequency.UDTFDeconv'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function lowpass as 'org.apache.iotdb.quality.frequency.UDTFLowPass'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function highpass as 'org.apache.iotdb.quality.frequency.UDTFHighPass'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function dwt as 'org.apache.iotdb.quality.frequency.UDTFDWT'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function idwt as 'org.apache.iotdb.quality.frequency.UDTFIDWT'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function ifft as 'org.apache.iotdb.quality.frequency.UDTFIFFT'"

# Series Discovery
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function consecutivesequences as 'org.apache.iotdb.quality.series.UDTFConsecutiveSequences'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function consecutivewindows as 'org.apache.iotdb.quality.series.UDTFConsecutiveWindows'"

# String Processing
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function regexsplit as 'org.apache.iotdb.quality.string.UDTFRegexSplit'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function regexmatch as 'org.apache.iotdb.quality.string.UDTFRegexMatch'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function strreplace as 'org.apache.iotdb.quality.string.UDTFStrReplace'"
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function regexreplace as 'org.apache.iotdb.quality.string.UDTFRegexReplace'"

