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

# Administration Management

IoTDB provides users with account privilege management operations, so as to ensure data security.

We will show you basic user privilege management operations through the following specific examples. Detailed SQL syntax and usage details can be found in [SQL Documentation](../Reference/SQL-Reference.md). 
At the same time, in the JAVA programming environment, you can use the [Java JDBC](../API/Programming-JDBC.md) to execute privilege management statements in a single or batch mode. 

## Basic Concepts

### User

The user is the legal user of the database. A user corresponds to a unique username and has a password as a means of authentication. Before using a database, a person must first provide a legitimate username and password to make himself/herself a user.

### Privilege

The database provides a variety of operations, and not all users can perform all operations. If a user can perform an operation, the user is said to have the privilege to perform the operation. privileges are divided into data management privilege (such as adding, deleting and modifying data) and authority management privilege (such as creation and deletion of users and roles, granting and revoking of privileges, etc.). Data management privilege often needs a path to limit its effective range. It is flexible that using [path pattern](../Data-Concept/Data-Model-and-Terminology.md) to manage privileges.

### Role

A role is a set of privileges and has a unique role name as an identifier. A user usually corresponds to a real identity (such as a traffic dispatcher), while a real identity may correspond to multiple users. These users with the same real identity tend to have the same privileges. Roles are abstractions that can unify the management of such privileges.

### Default User

There is a default user in IoTDB after the initial installation: root, and the default password is root. This user is an administrator user, who cannot be deleted and has all the privileges. Neither can new privileges be granted to the root user nor can privileges owned by the root user be deleted.

## Privilege Management Operation Examples

According to the [sample data](https://github.com/thulab/iotdb/files/4438687/OtherMaterial-Sample.Data.txt), the sample data of IoTDB might belong to different power generation groups such as ln, sgcc, etc. Different power generation groups do not want others to obtain their own database data, so we need to have data privilege isolated at the group layer.

### Create User

We use `CREATE USER <userName> <password>` to create users. For example, we can use root user who has all privileges to create two users for ln and sgcc groups, named ln\_write\_user and sgcc\_write\_user, with both passwords being write\_pwd. It is recommended to wrap the username in backtick(`). The SQL statement is:

```
CREATE USER `ln_write_user` 'write_pwd'
CREATE USER `sgcc_write_user` 'write_pwd'
```
Then use the following SQL statement to show the user:

```
LIST USER
```
As can be seen from the result shown below, the two users have been created:

```
IoTDB> CREATE USER `ln_write_user` 'write_pwd'
Msg: The statement is executed successfully.
IoTDB> CREATE USER `sgcc_write_user` 'write_pwd'
Msg: The statement is executed successfully.
IoTDB> LIST USER
+---------------+
|           user|
+---------------+
|  ln_write_user|
|           root|
|sgcc_write_user|
+---------------+
Total line number = 3
It costs 0.157s
```

### Grant User Privilege

At this point, although two users have been created, they do not have any privileges, so they can not operate on the database. For example, we use ln_write_user to write data in the database, the SQL statement is:

```
INSERT INTO root.ln.wf01.wt01(timestamp,status) values(1509465600000,true)
```
The SQL statement will not be executed and the corresponding error prompt is given as follows:

```
IoTDB> INSERT INTO root.ln.wf01.wt01(timestamp,status) values(1509465600000,true)
Msg: 602: No permissions for this operation, please add privilege INSERT_TIMESERIES.
```

Now, we use root user to grant the two users write privileges to the corresponding databases.

We use `GRANT USER <userName> PRIVILEGES <privileges> ON <nodeName>` to grant user privileges(ps: grant create user does not need path). For example:

```
GRANT USER `ln_write_user` PRIVILEGES INSERT_TIMESERIES on root.ln.**
GRANT USER `sgcc_write_user` PRIVILEGES INSERT_TIMESERIES on root.sgcc1.**, root.sgcc2.**
GRANT USER `ln_write_user` PRIVILEGES CREATE_USER
```
The execution result is as follows:

```
IoTDB> GRANT USER `ln_write_user` PRIVILEGES INSERT_TIMESERIES on root.ln.**
Msg: The statement is executed successfully.
IoTDB> GRANT USER `sgcc_write_user` PRIVILEGES INSERT_TIMESERIES on root.sgcc1.**, root.sgcc2.**
Msg: The statement is executed successfully.
IoTDB> GRANT USER `ln_write_user` PRIVILEGES CREATE_USER
Msg: The statement is executed successfully.
```

Next, use ln_write_user to try to write data again.
```
IoTDB> INSERT INTO root.ln.wf01.wt01(timestamp, status) values(1509465600000, true)
Msg: The statement is executed successfully.
```

### Revoker User Privilege

After granting user privileges, we could use `REVOKE USER <userName> PRIVILEGES <privileges> ON <nodeName>` to revoke the granted user privileges(ps: revoke create user does not need path). For example, use root user to revoke the privilege of ln_write_user and sgcc_write_user:

```
REVOKE USER `ln_write_user` PRIVILEGES INSERT_TIMESERIES on root.ln.**
REVOKE USER `sgcc_write_user` PRIVILEGES INSERT_TIMESERIES on root.sgcc1.**, root.sgcc2.**
REVOKE USER `ln_write_user` PRIVILEGES CREATE_USER
```

The execution result is as follows:

```
REVOKE USER `ln_write_user` PRIVILEGES INSERT_TIMESERIES on root.ln.**
Msg: The statement is executed successfully.
REVOKE USER `sgcc_write_user` PRIVILEGES INSERT_TIMESERIES on root.sgcc1.**, root.sgcc2.**
Msg: The statement is executed successfully.
REVOKE USER `ln_write_user` PRIVILEGES CREATE_USER
Msg: The statement is executed successfully.
```

After revoking, ln_write_user has no permission to writing data to root.ln.**
```
INSERT INTO root.ln.wf01.wt01(timestamp, status) values(1509465600000, true)
Msg: 602: No permissions for this operation, please add privilege INSERT_TIMESERIES.
```

### SQL Statements

Here are all related SQL statements:

* Create User

```
CREATE USER <userName> <password>;  
Eg: IoTDB > CREATE USER `thulab` 'pwd';
```

* Delete User

```
DROP USER <userName>;  
Eg: IoTDB > DROP USER `xiaoming`;
```

* Create Role

```
CREATE ROLE <roleName>;  
Eg: IoTDB > CREATE ROLE `admin`;
```

* Delete Role

```
DROP ROLE <roleName>;  
Eg: IoTDB > DROP ROLE `admin`;
```

* Grant User Privileges

```
GRANT USER <userName> PRIVILEGES <privileges> ON <nodeNames>;  
Eg: IoTDB > GRANT USER `tempuser` PRIVILEGES INSERT_TIMESERIES, DELETE_TIMESERIES on root.ln.**, root.sgcc.**;
Eg: IoTDB > GRANT USER `tempuser` PRIVILEGES CREATE_ROLE;
```

- Grant User All Privileges

```
GRANT USER <userName> PRIVILEGES ALL; 
Eg: IoTDB > GRANT USER `tempuser` PRIVILEGES ALL;
```

* Grant Role Privileges

```
GRANT ROLE <roleName> PRIVILEGES <privileges> ON <nodeNames>;  
Eg: IoTDB > GRANT ROLE `temprole` PRIVILEGES INSERT_TIMESERIES, DELETE_TIMESERIES ON root.sgcc.**, root.ln.**;
Eg: IoTDB > GRANT ROLE `temprole` PRIVILEGES CREATE_ROLE;
```

- Grant Role All Privileges

```
GRANT ROLE <roleName> PRIVILEGES ALL ON <nodeNames>;  
Eg: IoTDB > GRANT ROLE `temprole` PRIVILEGES ALL;
```

* Grant User Role

```
GRANT <roleName> TO <userName>;  
Eg: IoTDB > GRANT `temprole` TO tempuser;
```

* Revoke User Privileges

```
REVOKE USER <userName> PRIVILEGES <privileges> ON <nodeNames>;   
Eg: IoTDB > REVOKE USER `tempuser` PRIVILEGES DELETE_TIMESERIES on root.ln.**;
Eg: IoTDB > REVOKE USER `tempuser` PRIVILEGES CREATE_ROLE;
```

* Revoke User All Privileges

```
REVOKE USER <userName> PRIVILEGES ALL; 
Eg: IoTDB > REVOKE USER `tempuser` PRIVILEGES ALL;
```

* Revoke Role Privileges

```
REVOKE ROLE <roleName> PRIVILEGES <privileges> ON <nodeNames>;  
Eg: IoTDB > REVOKE ROLE `temprole` PRIVILEGES DELETE_TIMESERIES ON root.ln.**;
Eg: IoTDB > REVOKE ROLE `temprole` PRIVILEGES CREATE_ROLE;
```

* Revoke All Role Privileges

```
REVOKE ROLE <roleName> PRIVILEGES ALL;  
Eg: IoTDB > REVOKE ROLE `temprole` PRIVILEGES ALL;
```

* Revoke Role From User

```
REVOKE <roleName> FROM <userName>;
Eg: IoTDB > REVOKE `temprole` FROM tempuser;
```

* List Users

```
LIST USER
Eg: IoTDB > LIST USER
```

* List User of Specific Role

```
LIST USER OF ROLE <roleName>;
Eg: IoTDB > LIST USER OF ROLE `roleuser`;
```

* List Roles

```
LIST ROLE
Eg: IoTDB > LIST ROLE
```

* List Roles of Specific User

```
LIST ROLE OF USER <username> ;  
Eg: IoTDB > LIST ROLE OF USER `tempuser`;
```

* List All Privileges of Users

```
LIST PRIVILEGES USER <username> ;   
Eg: IoTDB > LIST PRIVILEGES USER `tempuser`;
```

* List Related Privileges of Users(On Specific Paths)

```
LIST PRIVILEGES USER <username> ON <paths>;
Eg: IoTDB> LIST PRIVILEGES USER `tempuser` ON root.ln.**, root.ln.wf01.**;
+--------+-----------------------------------+
|    role|                          privilege|
+--------+-----------------------------------+
|        |      root.ln.** : ALTER_TIMESERIES|
|temprole|root.ln.wf01.** : CREATE_TIMESERIES|
+--------+-----------------------------------+
Total line number = 2
It costs 0.005s
IoTDB> LIST PRIVILEGES USER `tempuser` ON root.ln.wf01.wt01.**;
+--------+-----------------------------------+
|    role|                          privilege|
+--------+-----------------------------------+
|        |      root.ln.** : ALTER_TIMESERIES|
|temprole|root.ln.wf01.** : CREATE_TIMESERIES|
+--------+-----------------------------------+
Total line number = 2
It costs 0.005s
```

* List All Privileges of Roles

```
LIST PRIVILEGES ROLE <roleName>
Eg: IoTDB > LIST PRIVILEGES ROLE `actor`;
```

* List Related Privileges of Roles(On Specific Paths)

```
LIST PRIVILEGES ROLE <roleName> ON <paths>;    
Eg: IoTDB> LIST PRIVILEGES ROLE `temprole` ON root.ln.**, root.ln.wf01.wt01.**;
+-----------------------------------+
|                          privilege|
+-----------------------------------+
|root.ln.wf01.** : CREATE_TIMESERIES|
+-----------------------------------+
Total line number = 1
It costs 0.005s
IoTDB> LIST PRIVILEGES ROLE `temprole` ON root.ln.wf01.wt01.**;
+-----------------------------------+
|                          privilege|
+-----------------------------------+
|root.ln.wf01.** : CREATE_TIMESERIES|
+-----------------------------------+
Total line number = 1
It costs 0.005s
```

* Alter Password

```
ALTER USER <username> SET PASSWORD <password>;
Eg: IoTDB > ALTER USER `tempuser` SET PASSWORD 'newpwd';
```


## Other Instructions

### The Relationship among Users, Privileges and Roles

A Role is a set of privileges, and privileges and roles are both attributes of users. That is, a role can have several privileges and a user can have several roles and privileges (called the user's own privileges).

At present, there is no conflicting privilege in IoTDB, so the real privileges of a user is the union of the user's own privileges and the privileges of the user's roles. That is to say, to determine whether a user can perform an operation, it depends on whether one of the user's own privileges or the privileges of the user's roles permits the operation. The user's own privileges and privileges of the user's roles may overlap, but it does not matter.

It should be noted that if users have a privilege (corresponding to operation A) themselves and their roles contain the same privilege, then revoking the privilege from the users themselves alone can not prohibit the users from performing operation A, since it is necessary to revoke the privilege from the role, or revoke the role from the user. Similarly, revoking the privilege from the users's roles alone can not prohibit the users from performing operation A.

At the same time, changes to roles are immediately reflected on all users who own the roles. For example, adding certain privileges to roles will immediately give all users who own the roles corresponding privileges, and deleting certain privileges will also deprive the corresponding users of the privileges (unless the users themselves have the privileges).

### List of Privileges Included in the System

**List of privileges Included in the System**

| privilege Name            | Interpretation                                                                                                                 | Example                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|:--------------------------|:-------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| CREATE\_DATABASE          | create database; set/unset database ttl; path dependent                                                                        | Eg1: `CREATE DATABASE root.ln;`<br />Eg2:`set ttl to root.ln 3600000;`<br />Eg3:`unset ttl to root.ln;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| DELETE\_DATABASE          | delete databases; path dependent                                                                                               | Eg: `delete database root.ln;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| CREATE\_TIMESERIES        | create timeseries; path dependent                                                                                              | Eg1: create timeseries<br />`create timeseries root.ln.wf02.status with datatype=BOOLEAN,encoding=PLAIN;`<br />Eg2: create aligned timeseries<br />`create aligned timeseries root.ln.device1(latitude FLOAT encoding=PLAIN compressor=SNAPPY, longitude FLOAT encoding=PLAIN compressor=SNAPPY);`                                                                                                                                                                                                                                                                                                                                                                               |
| INSERT\_TIMESERIES        | insert data; path dependent                                                                                                    | Eg1: `insert into root.ln.wf02(timestamp,status) values(1,true);`<br />Eg2: `insert into root.sg1.d1(time, s1, s2) aligned values(1, 1, 1)`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| ALTER\_TIMESERIES         | alter timeseries; path dependent                                                                                               | Eg1: `alter timeseries root.turbine.d1.s1 ADD TAGS tag3=v3, tag4=v4;`<br />Eg2: `ALTER timeseries root.turbine.d1.s1 UPSERT ALIAS=newAlias TAGS(tag2=newV2, tag3=v3) ATTRIBUTES(attr3=v3, attr4=v4);`                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| READ\_TIMESERIES          | query data; path dependent                                                                                                     | Eg1: `SHOW DATABASES;` <br />Eg2: `show child paths root.ln, show child nodes root.ln;`<br />Eg3: `show devices;`<br />Eg4: `show timeseries root.**;`<br />Eg5: `show schema templates;`<br />Eg6: `show all ttl`<br />Eg7: [Query-Data](../Query-Data/Overview.md)（The query statements under this section all use this permission）<br />Eg8: CVS format data export<br />`./export-csv.bat -h 127.0.0.1 -p 6667 -u tempuser -pw root -td ./`<br />Eg9: Performance Tracing Tool<br />`tracing select * from root.**`<br />Eg10: UDF-Query<br />`select example(*) from root.sg.d1`<br />Eg11: Triggers-Query<br />`show triggers`<br />Eg12: Count-Query<br />`count devices` |
| DELETE\_TIMESERIES        | delete data or timeseries; path dependent                                                                                      | Eg1: delete timeseries<br />`delete timeseries root.ln.wf01.wt01.status`<br />Eg2: delete data<br />`delete from root.ln.wf02.wt02.status where time < 10`<br />Eg3: use drop semantic<br />`drop timeseries root.ln.wf01.wt01.status                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| CREATE\_USER              | create users; path independent                                                                                                 | Eg: `create user thulab 'passwd';`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| DELETE\_USER              | delete users; path independent                                                                                                 | Eg: `drop user xiaoming;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| MODIFY\_PASSWORD          | modify passwords for all users; path independent; (Those who do not have this privilege can still change their own asswords. ) | Eg: `alter user tempuser SET PASSWORD 'newpwd';`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| LIST\_USER                | list all users; list all user of specific role; list a user's related privileges on speciific paths; path independent          | Eg1: `list user;`<br />Eg2: `list user of role 'wirte_role';`<br />Eg3: `list privileges user admin;`<br />Eg4: `list privileges user 'admin' on root.sgcc.**;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| GRANT\_USER\_PRIVILEGE    | grant user privileges; path independent                                                                                        | Eg:  `grant user tempuser privileges DELETE_TIMESERIES on root.ln.**;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| REVOKE\_USER\_PRIVILEGE   | revoke user privileges; path independent                                                                                       | Eg:  `revoke user tempuser privileges DELETE_TIMESERIES on root.ln.**;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| GRANT\_USER\_ROLE         | grant user roles; path independent                                                                                             | Eg:  `grant temprole to tempuser;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| REVOKE\_USER\_ROLE        | revoke user roles; path independent                                                                                            | Eg:  `revoke temprole from tempuser;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| CREATE\_ROLE              | create roles; path independent                                                                                                 | Eg:  `create role admin;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| DELETE\_ROLE              | delete roles; path independent                                                                                                 | Eg: `drop role admin;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| LIST\_ROLE                | list all roles; list all roles of specific user; list a role's related privileges on speciific paths; path independent         | Eg1: `list role`<br />Eg2: `list role of user 'actor';`<br />Eg3: `list privileges role wirte_role;`<br />Eg4: `list privileges role wirte_role ON root.sgcc;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| GRANT\_ROLE\_PRIVILEGE    | grant role privileges; path independent                                                                                        | Eg: `grant role temprole privileges DELETE_TIMESERIES ON root.ln.**;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| REVOKE\_ROLE\_PRIVILEGE   | revoke role privileges; path independent                                                                                       | Eg: `revoke role temprole privileges DELETE_TIMESERIES ON root.ln.**;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| CREATE_FUNCTION           | register UDFs; path independent                                                                                                | Eg: `create function example AS 'org.apache.iotdb.udf.UDTFExample';`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| DROP_FUNCTION             | deregister UDFs; path independent                                                                                              | Eg: `drop function example`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| CREATE_TRIGGER            | create triggers; path dependent                                                                                                | Eg1: `CREATE TRIGGER <TRIGGER-NAME> BEFORE INSERT ON <FULL-PATH> AS <CLASSNAME>`<br />Eg2: `CREATE TRIGGER <TRIGGER-NAME> AFTER INSERT ON <FULL-PATH> AS <CLASSNAME>`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| DROP_TRIGGER              | drop triggers; path dependent                                                                                                  | Eg: `drop trigger 'alert-listener-sg1d1s1'`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| CREATE_CONTINUOUS_QUERY   | create continuous queries; path independent                                                                                    | Eg: `select s1, s1 into t1, t2 from root.sg.d1`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| DROP_CONTINUOUS_QUERY     | drop continuous queries; path independent                                                                                      | Eg1: `DROP CONTINUOUS QUERY cq3`<br />Eg2: `DROP CQ cq3`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| SHOW_CONTINUOUS_QUERIES   | show continuous queries; path independent                                                                                      | Eg1: `SHOW CONTINUOUS QUERIES`<br />Eg2: `SHOW cqs`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| UPDATE_TEMPLATE           | create and drop schema template; path independent                                                                              | Eg1: `create schema template t1(s1 int32)`<br />Eg2: `drop schema template t1`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| READ_TEMPLATE             | show schema templates and show nodes in schema template; path independent                                                      | Eg1: `show schema templates`<br/>Eg2: `show nodes in template t1`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | 
| APPLY_TEMPLATE            | set, unset and activate schema template; path dependent                                                                        | Eg1: `set schema template t1 to root.sg.d`<br/>Eg2: `unset schema template t1 from root.sg.d`<br/>Eg3: `create timeseries of schema template on root.sg.d`<br/>Eg4: `delete timeseries of schema template on root.sg.d`                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| READ_TEMPLATE_APPLICATION | show paths set and using schema template; path independent                                                                     | Eg1: `show paths set schema template t1`<br/>Eg2: `show paths using schema template t1`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |

Note that path dependent privileges can only be granted or revoked on root.**;

Note that the following SQL statements need to be granted multiple permissions before they can be used：

- Import data: Need to assign `READ_TIMESERIES`，`INSERT_TIMESERIES` two permissions.。

```
Eg: IoTDB > ./import-csv.bat -h 127.0.0.1 -p 6667 -u renyuhua -pw root -f dump0.csv
```

-  Query Write-back (SELECT INTO)
- - `READ_TIMESERIES` permission of source sequence in all `select` clauses is required
  - `INSERT_TIMESERIES` permission of target sequence in all `into` clauses is required 

```
Eg: IoTDB > select s1, s1 into t1, t2 from root.sg.d1 limit 5 offset 1000
```

### Username Restrictions

IoTDB specifies that the character length of a username should not be less than 4, and the username cannot contain spaces.

### Password Restrictions

IoTDB specifies that the character length of a password should have no less than 4 character length, and no spaces. The password is encrypted with MD5.

### Role Name Restrictions

IoTDB specifies that the character length of a role name should have no less than 4 character length, and no spaces.

### Path pattern in Administration Management

A path pattern's result set contains all the elements of its sub pattern's
result set. For example, `root.sg.d.*` is a sub pattern of
`root.sg.*.*`, while `root.sg.**` is not a sub pattern of
`root.sg.*.*`. When a user is granted privilege on a pattern, the pattern used in his DDL or DML must be a sub pattern of the privilege pattern, which guarantees that the user won't access the timeseries exceed his privilege scope.

### Permission cache

In distributed related permission operations, when changing permissions other than creating users and roles, all the cache information of `dataNode` related to the user (role) will be cleared first. If any `dataNode` cache information is clear and fails, the permission change task will fail.

### Operations restricted by non root users

At present, the following SQL statements supported by iotdb can only be operated by the `root` user, and no corresponding permission can be given to the new user.

###### TsFile Management

- Load TsFiles

```
Eg: IoTDB > load '/Users/Desktop/data/1575028885956-101-0.tsfile'
```

- remove a tsfile

```
Eg: IoTDB > remove '/Users/Desktop/data/data/root.vehicle/0/0/1575028885956-101-0.tsfile'
```

- unload a tsfile and move it to a target directory

```
Eg: IoTDB > unload '/Users/Desktop/data/data/root.vehicle/0/0/1575028885956-101-0.tsfile' '/data/data/tmp'
```

###### Delete Time Partition (experimental)

- Delete Time Partition (experimental)

```
Eg: IoTDB > DELETE PARTITION root.ln 0,1,2
```

###### Continuous Query,CQ

- Continuous Query,CQ

```
Eg: IoTDB > CREATE CONTINUOUS QUERY cq1 BEGIN SELECT max_value(temperature) INTO temperature_max FROM root.ln.*.* GROUP BY time(10s) END
```

###### Maintenance Command

- FLUSH

```
Eg: IoTDB > flush
```

- MERGE

```
Eg: IoTDB > MERGE
Eg: IoTDB > FULL MERGE
```

- CLEAR CACHE

```sql
Eg: IoTDB > CLEAR CACHE
```

- SET STSTEM TO READONLY / WRITABLE

```
Eg: IoTDB > SET STSTEM TO READONLY / WRITABLE
```

- SCHEMA SNAPSHOT

```sql
Eg: IoTDB > CREATE SNAPSHOT FOR SCHEMA
```

- Query abort

```
Eg: IoTDB > KILL QUERY 1
```

###### Watermark Tool

- Watermark new users 

```
Eg: IoTDB > grant watermark_embedding to Alice
```

- Watermark Detection

```
Eg: IoTDB > revoke watermark_embedding from Alice
```

