<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
# Windows desktop acceptance

Use a verified Windows x64 release ZIP, a disposable IoTDB server, 64-bit
Microsoft Excel and 64-bit Power BI Desktop. Office activation credentials
never belong in connection strings, test data, screenshots or repository files.
Power BI service publication is not required for this local test.

## Prepare the server and DSN

1. Record the ZIP SHA-512, `BUILD-INFO.txt` and full source commit. Run
   `test/verify_release.py` before extracting the ZIP.
2. Start a disposable server with its RPC endpoint reachable from Windows.
   Do not reuse production credentials or change an existing database.
3. In the IoTDB table-model CLI, use a new database name and run:

```sql
CREATE DATABASE odbc_desktop_validation;
USE odbc_desktop_validation;
CREATE TABLE samples (device STRING TAG, val INT32 FIELD, txt STRING FIELD, day DATE FIELD, active BOOLEAN FIELD);
INSERT INTO samples(time,device,val,txt,day,active) VALUES
  (1,'设备',1,'中文','2024-02-29',true),
  (2,'设备',2,null,'2024-03-01',false),
  (3,'设备',3,'','2024-03-02',true),
  (4,'设备',4,'last','2024-03-03',true);
```

4. Install the extracted driver using `install.bat` as described in the README.
   In the **64-bit** ODBC Data Source Administrator, create a new DSN named
   `IoTDB Desktop Validation` with the disposable server's host, RPC port,
   database `odbc_desktop_validation`, `ISTABLEMODEL=1`, and `BATCHSIZE=2`.
   Supply the test server credentials locally. Set TLS attributes according to
   that server's configuration. This DSN should not replace an existing DSN.

## Excel

1. Record Excel version/build and 64-bit architecture from **About Excel**.
2. Open a blank workbook. Choose **Data > Get Data > From Other Sources >
   From ODBC**, then select the validation DSN.
3. Expand Navigator and select `samples`. Confirm the preview shows four rows.
   Load into a worksheet and confirm Chinese text, integer values, dates,
   booleans and the four rows across two Session batches.
4. Independently test the advanced SQL option with:

```sql
SELECT time,device,val,txt,day,active FROM samples ORDER BY time
```

5. Save the workbook locally as `.xlsx`. Perform the refresh check below,
   close and reopen the workbook, then refresh again.

## Power BI Desktop

1. Record Power BI Desktop version and architecture from **About**.
2. Create a blank report. Choose **Get data > More > Other > ODBC** and select
   the validation DSN. Use Import mode where offered.
3. Expand Navigator, select `samples`, inspect its four-row preview and load.
4. In Table view, confirm row count and values. Also test the explicit SQL
   above through the connector's advanced options in a separate query.
5. Save locally as `.pbix`, perform the refresh check, then reopen and refresh.
   No cloud sign-in or report publication is needed for this acceptance test.

## Refresh and value checks

After both applications have loaded the initial four rows, insert from the CLI:

```sql
INSERT INTO samples(time,device,val,txt,day,active)
VALUES (5,'设备',5,'刷新验证','2024-03-04',true);
```

Refresh both applications: each must now show five rows including `刷新验证`.
`time` has epoch-integer semantics; do not expect an automatic date conversion.
Check `day` as DATE and `active` as BOOLEAN. Inspect NULL versus empty string
in Power Query, since both can appear blank in an Excel worksheet. If Navigator
fails but explicit SQL succeeds, report those results separately.

## Evidence and completion

Record each item as PASS, FAIL or NOT RUN; include any diagnostic/SQLSTATE.

| Evidence | Excel | Power BI Desktop |
| --- | --- | --- |
| Version/build and architecture | | |
| DSN connection | | |
| Navigator database/table browsing | | |
| Preview and four-row load | | |
| Explicit SQL and Unicode/type checks | | |
| Refresh after insert: five rows | | |
| Saved file reopened and refreshed | | |

Keep local `.xlsx`/`.pbix` files and screenshots of loaded data and refresh
results. Exclude passwords, activation/account screens and unrelated user data.
Record the server version and driver commit with the evidence. Automated
ODBC/.NET/ADO results and a green package CI do not substitute for this matrix.
If Office activation, a locked desktop or an unavailable application prevents
testing, record NOT RUN and the blocker rather than reporting success.
