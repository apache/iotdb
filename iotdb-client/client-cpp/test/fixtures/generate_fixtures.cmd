@echo off
REM Licensed to the Apache Software Foundation (ASF) under one
REM or more contributor license agreements.  See the NOTICE file
REM distributed with this work for additional information
REM regarding copyright ownership.  The ASF licenses this file
REM to you under the Apache License, Version 2.0 (the
REM "License"); you may not use this file except in compliance
REM with the License.  You may obtain a copy of the License at
REM
REM     http://www.apache.org/licenses/LICENSE-2.0
REM
REM Unless required by applicable law or agreed to in writing,
REM software distributed under the License is distributed on an
REM "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
REM KIND, either express or implied.  See the License for the
REM specific language governing permissions and limitations
REM under the License.
REM
REM Regenerate TLS/TLCP PKCS12 and PEM fixtures using the bundled Tongsuo openssl.
REM Usage (from client-cpp/test/fixtures, after cmake build):
REM   generate_fixtures.cmd [path\to\openssl.exe]

@echo off
setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
set "TLS_DIR=%SCRIPT_DIR%tls"
set "TLCP_DIR=%SCRIPT_DIR%tlcp"
set "OPENSSL=%~1"
if "%OPENSSL%"=="" set "OPENSSL=..\..\target\build\_deps\tongsuo\install\bin\openssl.exe"
if not exist "%OPENSSL%" (
  echo OpenSSL executable not found: %OPENSSL%
  exit /b 1
)

set "PASS=thrift"
mkdir "%TLS_DIR%" 2>nul
mkdir "%TLCP_DIR%" 2>nul

echo [fixtures] generating TLS RSA fixtures...
"%OPENSSL%" genrsa -out "%TLS_DIR%\ca.key" 2048
"%OPENSSL%" req -new -x509 -days 3650 -key "%TLS_DIR%\ca.key" -out "%TLS_DIR%\ca.crt" -subj "/CN=IoTDB Test CA"
"%OPENSSL%" genrsa -out "%TLS_DIR%\server.key" 2048
"%OPENSSL%" req -new -key "%TLS_DIR%\server.key" -out "%TLS_DIR%\server.csr" -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
"%OPENSSL%" x509 -req -days 3650 -in "%TLS_DIR%\server.csr" -CA "%TLS_DIR%\ca.crt" -CAkey "%TLS_DIR%\ca.key" -CAcreateserial -out "%TLS_DIR%\server.crt" -copy_extensions copy
"%OPENSSL%" genrsa -out "%TLS_DIR%\client.key" 2048
"%OPENSSL%" req -new -key "%TLS_DIR%\client.key" -out "%TLS_DIR%\client.csr" -subj "/CN=IoTDB Test Client"
"%OPENSSL%" x509 -req -days 3650 -in "%TLS_DIR%\client.csr" -CA "%TLS_DIR%\ca.crt" -CAkey "%TLS_DIR%\ca.key" -CAcreateserial -out "%TLS_DIR%\client.crt"
"%OPENSSL%" pkcs12 -export -nokeys -in "%TLS_DIR%\ca.crt" -out "%TLS_DIR%\tls-trust.p12" -password pass:%PASS%
"%OPENSSL%" pkcs12 -export -in "%TLS_DIR%\client.crt" -inkey "%TLS_DIR%\client.key" -out "%TLS_DIR%\tls-client.p12" -password pass:%PASS% -name client
"%OPENSSL%" pkcs12 -export -in "%TLS_DIR%\server.crt" -inkey "%TLS_DIR%\server.key" -out "%TLS_DIR%\tls-server.p12" -password pass:%PASS% -name server

echo [fixtures] generating TLCP SM2 fixtures...
"%OPENSSL%" ecparam -genkey -name SM2 -out "%TLCP_DIR%\ca.key"
"%OPENSSL%" req -new -x509 -days 3650 -key "%TLCP_DIR%\ca.key" -out "%TLCP_DIR%\ca.crt" -subj "/CN=IoTDB TLCP CA" -sm3
for %%R in (client server) do (
  "%OPENSSL%" ecparam -genkey -name SM2 -out "%TLCP_DIR%\%%R_sign.key"
  "%OPENSSL%" req -new -key "%TLCP_DIR%\%%R_sign.key" -out "%TLCP_DIR%\%%R_sign.csr" -subj "/CN=%%R sign" -sm3
  "%OPENSSL%" x509 -req -days 3650 -in "%TLCP_DIR%\%%R_sign.csr" -CA "%TLCP_DIR%\ca.crt" -CAkey "%TLCP_DIR%\ca.key" -CAcreateserial -out "%TLCP_DIR%\%%R_sign.crt" -sm3
  "%OPENSSL%" ecparam -genkey -name SM2 -out "%TLCP_DIR%\%%R_enc.key"
  "%OPENSSL%" req -new -key "%TLCP_DIR%\%%R_enc.key" -out "%TLCP_DIR%\%%R_enc.csr" -subj "/CN=%%R enc" -sm3
  "%OPENSSL%" x509 -req -days 3650 -in "%TLCP_DIR%\%%R_enc.csr" -CA "%TLCP_DIR%\ca.crt" -CAkey "%TLCP_DIR%\ca.key" -CAcreateserial -out "%TLCP_DIR%\%%R_enc.crt" -sm3
)
"%OPENSSL%" pkcs12 -export -nokeys -in "%TLCP_DIR%\ca.crt" -out "%TLCP_DIR%\tlcp-trust.p12" -password pass:%PASS%
"%OPENSSL%" pkcs12 -export -in "%TLCP_DIR%\client_sign.crt" -inkey "%TLCP_DIR%\client_sign.key" -out "%TLCP_DIR%\tlcp-client-sign.p12" -password pass:%PASS% -name "client.sign"
"%OPENSSL%" pkcs12 -export -in "%TLCP_DIR%\client_enc.crt" -inkey "%TLCP_DIR%\client_enc.key" -out "%TLCP_DIR%\tlcp-client-enc.p12" -password pass:%PASS% -name "client.enc"

del /q "%TLS_DIR%\*.csr" "%TLS_DIR%\*.srl" "%TLCP_DIR%\*.csr" "%TLCP_DIR%\*.srl" 2>nul
echo [fixtures] done. Password for all PKCS12 files: %PASS%
