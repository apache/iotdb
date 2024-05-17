//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

using System.Data.Odbc;

// Get a connection
var dbConnection = new OdbcConnection("DSN=ZappySys JDBC Bridge");
dbConnection.Open();

// Execute the write commands to prepare data
var dbCommand = dbConnection.CreateCommand();
dbCommand.CommandText = "insert into root.Keller.Flur.Energieversorgung(time, s1) values(1715670861634, 1)";
dbCommand.ExecuteNonQuery();
dbCommand.CommandText = "insert into root.Keller.Flur.Energieversorgung(time, s2) values(1715670861634, true)";
dbCommand.ExecuteNonQuery();
dbCommand.CommandText = "insert into root.Keller.Flur.Energieversorgung(time, s3) values(1715670861634, 3.1)";
dbCommand.ExecuteNonQuery();

// Execute the read command
dbCommand.CommandText = "SELECT * FROM root.Keller.Flur.Energieversorgung";
var dbReader = dbCommand.ExecuteReader();

// Write the output header
var fCount = dbReader.FieldCount;
Console.Write(":");
for(var i = 0; i < fCount; i++)
{
    var fName = dbReader.GetName(i);
    Console.Write(fName + ":");
}
Console.WriteLine();

// Output the content
while (dbReader.Read())
{
    Console.Write(":");
    for(var i = 0; i < fCount; i++) 
    {
        var fieldType = dbReader.GetFieldType(i);
        switch (fieldType.Name)
        {
            case "DateTime":
                var dateTime = dbReader.GetInt64(i);
                Console.Write(dateTime + ":");
                break;
            case "Double":
                if (dbReader.IsDBNull(i)) 
                {
                    Console.Write("null:");
                }
                else 
                {
                    var fValue = dbReader.GetDouble(i);
                    Console.Write(fValue + ":");
                }   
                break;
            default:
                Console.Write(fieldType.Name + ":");
                break;
        }
    }
    Console.WriteLine();
}

// Shut down gracefully
dbReader.Close();
dbCommand.Dispose();
dbConnection.Close();
