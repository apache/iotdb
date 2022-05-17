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
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function distinct as 'org.apache.iotdb.quality.dprofile.UDTFDistinct'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function histogram as 'org.apache.iotdb.quality.dprofile.UDTFHistogram'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function integral as 'org.apache.iotdb.quality.dprofile.UDAFIntegral'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function integralavg as 'org.apache.iotdb.quality.dprofile.UDAFIntegralAvg'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function mad as 'org.apache.iotdb.quality.dprofile.UDAFMad'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function median as 'org.apache.iotdb.quality.dprofile.UDAFMedian'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function mode as 'org.apache.iotdb.quality.dprofile.UDAFMode'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function percentile as 'org.apache.iotdb.quality.dprofile.UDAFPercentile'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function period as 'org.apache.iotdb.quality.dprofile.UDAFPeriod'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function qlb as 'org.apache.iotdb.quality.dprofile.UDTFQLB'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function re_sample as 'org.apache.iotdb.quality.dprofile.UDTFResample'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function sample as 'org.apache.iotdb.quality.dprofile.UDTFSample'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function segment as 'org.apache.iotdb.quality.dprofile.UDTFSegment'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function skew as 'org.apache.iotdb.quality.dprofile.UDAFSkew'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function spread as 'org.apache.iotdb.quality.dprofile.UDAFSpread'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function stddev as 'org.apache.iotdb.quality.dprofile.UDAFStddev'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function minmax as 'org.apache.iotdb.quality.dprofile.UDTFMinMax'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function zscore as 'org.apache.iotdb.quality.dprofile.UDTFZScore'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function spline as 'org.apache.iotdb.quality.dprofile.UDTFSpline'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function mvavg as 'org.apache.iotdb.quality.dprofile.UDTFMvAvg'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function acf as 'org.apache.iotdb.quality.dprofile.UDTFACF'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function pacf as 'org.apache.iotdb.quality.dprofile.UDTFPACF'"


@REM Data Quality
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function completeness as 'org.apache.iotdb.quality.dquality.UDTFCompleteness'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function consistency as 'org.apache.iotdb.quality.dquality.UDTFConsistency'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function timeliness as 'org.apache.iotdb.quality.dquality.UDTFTimeliness'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function validity as 'org.apache.iotdb.quality.dquality.UDTFValidity'"


@REM Data Repairing
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function timestamprepair as 'org.apache.iotdb.quality.drepair.UDTFTimestampRepair'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function valuerepair as 'org.apache.iotdb.quality.drepair.UDTFValueRepair'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function valuefill as 'org.apache.iotdb.quality.drepair.UDTFValueFill'"


@REM Data Matching
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function cov as 'org.apache.iotdb.quality.dmatch.UDAFCov'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function xcorr as 'org.apache.iotdb.quality.dmatch.UDTFXCorr'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function dtw as 'org.apache.iotdb.quality.dmatch.UDAFDtw'"
call ../bin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function ptnsym as 'org.apache.iotdb.quality.dmatch.UDTFPtnSym'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function pearson as 'org.apache.iotdb.quality.dmatch.UDAFPearson'"


@REM Anomaly Detection
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function ksigma as 'org.apache.iotdb.quality.anomaly.UDTFKSigma'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function lof as 'org.apache.iotdb.quality.anomaly.UDTFLOF'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function range as 'org.apache.iotdb.quality.anomaly.UDTFRange'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function iqr as 'org.apache.iotdb.quality.anomaly.UDTFIQR'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function twosidedfilter as 'org.apache.iotdb.quality.anomaly.UDTFTwoSidedFilter'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function missdetect as 'org.apache.iotdb.quality.anomaly.UDTFMissDetect'"


@REM Frequency Domain
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function fft as 'org.apache.iotdb.quality.frequency.UDTFFFT'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function stft as 'org.apache.iotdb.quality.frequency.UDTFSTFT'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function conv as 'org.apache.iotdb.quality.frequency.UDTFConv'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function deconv as 'org.apache.iotdb.quality.frequency.UDTFDeconv'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function lowpass as 'org.apache.iotdb.quality.frequency.UDTFLowPass'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function highpass as 'org.apache.iotdb.quality.frequency.UDTFHighPass'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function dwt as 'org.apache.iotdb.quality.frequency.UDTFDWT'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function idwt as 'org.apache.iotdb.quality.frequency.UDTFIDWT'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function ifft as 'org.apache.iotdb.quality.frequency.UDTFIFFT'"


@REM Series Discovery
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function consecutivesequences as 'org.apache.iotdb.quality.series.UDTFConsecutiveSequences'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function consecutivewindows as 'org.apache.iotdb.quality.series.UDTFConsecutiveWindows'"

@REM String Processing
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function regexsplit as 'org.apache.iotdb.quality.string.UDTFRegexSplit'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function regexmatch as 'org.apache.iotdb.quality.string.UDTFRegexMatch'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function strreplace as 'org.apache.iotdb.quality.string.UDTFStrReplace'"
call ../sbin/start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create function regexreplace as 'org.apache.iotdb.quality.string.UDTFRegexReplace'"
