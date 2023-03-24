@REM
@REM Licensed to the Apache Software Foundation (ASF) under one
@REM or more contributor license agreements.  See the NOTICE file
@REM distributed with this work for additional information
@REM regarding copyright ownership.  The ASF licenses this file
@REM to you under the Apache License, Version 2.0 (the
@REM "License"); you may not use this file except in compliance
@REM with the License.  You may obtain a copy of the License at
@REM
@REM     http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing,
@REM software distributed under the License is distributed on an
@REM "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@REM KIND, either express or implied.  See the License for the
@REM specific language governing permissions and limitations
@REM under the License.
@REM

@REM Parameters
@set host=127.0.0.1
@set rpcPort=6667
@set user=root
@set pass=root


@REM Data Profiling
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function distinct as 'org.apache.iotdb.library.dprofile.UDTFDistinct'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function histogram as 'org.apache.iotdb.library.dprofile.UDTFHistogram'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function integral as 'org.apache.iotdb.library.dprofile.UDAFIntegral'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function integralavg as 'org.apache.iotdb.library.dprofile.UDAFIntegralAvg'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function mad as 'org.apache.iotdb.library.dprofile.UDAFMad'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function median as 'org.apache.iotdb.library.dprofile.UDAFMedian'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function percentile as 'org.apache.iotdb.library.dprofile.UDAFPercentile'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function quantile as 'org.apache.iotdb.library.dprofile.UDAFQuantile'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function period as 'org.apache.iotdb.library.dprofile.UDAFPeriod'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function qlb as 'org.apache.iotdb.library.dprofile.UDTFQLB'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function re_sample as 'org.apache.iotdb.library.dprofile.UDTFResample'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function sample as 'org.apache.iotdb.library.dprofile.UDTFSample'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function segment as 'org.apache.iotdb.library.dprofile.UDTFSegment'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function skew as 'org.apache.iotdb.library.dprofile.UDAFSkew'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function spread as 'org.apache.iotdb.library.dprofile.UDAFSpread'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function stddev as 'org.apache.iotdb.library.dprofile.UDAFStddev'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function minmax as 'org.apache.iotdb.library.dprofile.UDTFMinMax'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function zscore as 'org.apache.iotdb.library.dprofile.UDTFZScore'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function spline as 'org.apache.iotdb.library.dprofile.UDTFSpline'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function mvavg as 'org.apache.iotdb.library.dprofile.UDTFMvAvg'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function acf as 'org.apache.iotdb.library.dprofile.UDTFACF'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function pacf as 'org.apache.iotdb.library.dprofile.UDTFPACF'"


@REM Data Quality
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function completeness as 'org.apache.iotdb.library.dquality.UDTFCompleteness'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function consistency as 'org.apache.iotdb.library.dquality.UDTFConsistency'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function timeliness as 'org.apache.iotdb.library.dquality.UDTFTimeliness'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function validity as 'org.apache.iotdb.library.dquality.UDTFValidity'"


@REM Data Repairing
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function timestamprepair as 'org.apache.iotdb.library.drepair.UDTFTimestampRepair'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function valuerepair as 'org.apache.iotdb.library.drepair.UDTFValueRepair'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function valuefill as 'org.apache.iotdb.library.drepair.UDTFValueFill'"


@REM Data Matching
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function cov as 'org.apache.iotdb.library.dmatch.UDAFCov'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function xcorr as 'org.apache.iotdb.library.dmatch.UDTFXCorr'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function dtw as 'org.apache.iotdb.library.dmatch.UDAFDtw'"
call ../bin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function ptnsym as 'org.apache.iotdb.library.dmatch.UDTFPtnSym'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function pearson as 'org.apache.iotdb.library.dmatch.UDAFPearson'"


@REM Anomaly Detection
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function ksigma as 'org.apache.iotdb.library.anomaly.UDTFKSigma'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function lof as 'org.apache.iotdb.library.anomaly.UDTFLOF'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function range as 'org.apache.iotdb.library.anomaly.UDTFRange'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function iqr as 'org.apache.iotdb.library.anomaly.UDTFIQR'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function twosidedfilter as 'org.apache.iotdb.library.anomaly.UDTFTwoSidedFilter'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function missdetect as 'org.apache.iotdb.library.anomaly.UDTFMissDetect'"


@REM Frequency Domain
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function fft as 'org.apache.iotdb.library.frequency.UDTFFFT'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function conv as 'org.apache.iotdb.library.frequency.UDTFConv'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function deconv as 'org.apache.iotdb.library.frequency.UDTFDeconv'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function lowpass as 'org.apache.iotdb.library.frequency.UDTFLowPass'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function highpass as 'org.apache.iotdb.library.frequency.UDTFHighPass'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function dwt as 'org.apache.iotdb.library.frequency.UDTFDWT'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function idwt as 'org.apache.iotdb.library.frequency.UDTFIDWT'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function ifft as 'org.apache.iotdb.library.frequency.UDTFIFFT'"


@REM Series Discovery
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function consecutivesequences as 'org.apache.iotdb.library.series.UDTFConsecutiveSequences'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function consecutivewindows as 'org.apache.iotdb.library.series.UDTFConsecutiveWindows'"

@REM String Processing
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function regexsplit as 'org.apache.iotdb.library.string.UDTFRegexSplit'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function regexmatch as 'org.apache.iotdb.library.string.UDTFRegexMatch'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function strreplace as 'org.apache.iotdb.library.string.UDTFStrReplace'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function regexreplace as 'org.apache.iotdb.library.string.UDTFRegexReplace'"
