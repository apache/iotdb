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

# Chapter 4: Operation Manual

## Priviledge Management
IoTDB provides users with priviledge management operations, so as to ensure data security.

We will show you basic user priviledge management operations through the following specific examples. Detailed SQL syntax and usage details can be found in [SQL Documentation](/#/Documents/progress/chap4/sec7). At the same time, in the JAVA programming environment, you can use the [Java JDBC](/#/Documents/progress/chap6/sec1) to execute priviledge management statements in a single or batch mode. 

### Basic Concepts
#### User
The user is the legal user of the database. A user corresponds to a unique username and has a password as a means of authentication. Before using a database, a person must first provide a legitimate username and password to make himself/herself a user.

#### Priviledge
The database provides a variety of operations, and not all users can perform all operations. If a user can perform an operation, the user is said to have the priviledge to perform the operation. Priviledges can be divided into data management priviledge (such as adding, deleting and modifying data) and authority management priviledge (such as creation and deletion of users and roles, granting and revoking of priviledges, etc.). Data management priviledge often needs a path to limit its effective range, which is a subtree rooted at the path's corresponding node.

#### Role
A role is a set of priviledges and has a unique role name as an identifier. A user usually corresponds to a real identity (such as a traffic dispatcher), while a real identity may correspond to multiple users. These users with the same real identity tend to have the same priviledges. Roles are abstractions that can unify the management of such priviledges.

#### Default User
There is a default user in IoTDB after the initial installation: root, and the default password is root. This user is an administrator user, who cannot be deleted and has all the priviledges. Neither can new priviledges be granted to the root user nor can priviledges owned by the root user be deleted.

### Priviledge Management Operation Examples
According to the [sample data](https://raw.githubusercontent.com/apache/incubator-iotdb/master/docs/Documentation/OtherMaterial-Sample%20Data.txt), the sample data of IoTDB may belong to different power generation groups such as ln, sgcc, etc. Different power generation groups do not want others to obtain their own database data, so we need to have data priviledge isolated at the group layer.

#### Create User

We can create two users for ln and sgcc groups, named ln\_write\_user and sgcc\_write\_user, with both passwords being write\_pwd. The SQL statement is:

```
CREATE USER ln_write_user write_pwd
CREATE USER sgcc_write_user write_pwd
```
Then use the following SQL statement to show the user:

```
LIST USER
```
As can be seen from the result shown below, the two users have been created:

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51578263-e2a91d00-1ef7-11e9-94e8-28819b6fea87.jpg"></center>

#### Grant User Priviledge
At this point, although two users have been created, they do not have any priviledges, so they can not operate on the database. For example, we use ln_write_user to write data in the database, the SQL statement is:

```
INSERT INTO root.ln.wf01.wt01(timestamp,status) values(1509465600000,true)
```
The SQL statement will not be executed and the corresponding error prompt is given as follows:

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51597609-9af5b600-1f36-11e9-9460-8ab185eb4735.png"></center>

Now, we grant the two users write priviledges to the corresponding storage groups, and try to write data again. The SQL statement is:

```
GRANT USER ln_write_user PRIVILEGES 'INSERT_TIMESERIES' on root.ln
GRANT USER sgcc_write_user PRIVILEGES 'INSERT_TIMESERIES' on root.sgcc
INSERT INTO root.ln.wf01.wt01(timestamp, status) values(1509465600000, true)
```
The execution result is as follows:
<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51578942-33ba1080-1efa-11e9-891c-09d69791aff1.jpg"></center>

### Other Instructions
#### The Relationship among Users, Priviledges and Roles

A Role is a set of priviledges, and priviledges and roles are both attributes of users. That is, a role can have several priviledges and a user can have several roles and priviledges (called the user's own priviledges).

At present, there is no conflicting priviledge in IoTDB, so the real priviledges of a user is the union of the user's own priviledges and the priviledges of the user's roles. That is to say, to determine whether a user can perform an operation, it depends on whether one of the user's own priviledges or the priviledges of the user's roles permits the operation. The user's own priviledges and priviledges of the user's roles may overlap, but it does not matter.

It should be noted that if users have a priviledge (corresponding to operation A) themselves and their roles contain the same priviledge, then revoking the priviledge from the users themselves alone can not prohibit the users from performing operation A, since it is necessary to revoke the priviledge from the role, or revoke the role from the user. Similarly, revoking the priviledge from the users's roles alone can not prohibit the users from performing operation A.

At the same time, changes to roles are immediately reflected on all users who own the roles. For example, adding certain priviledges to roles will immediately give all users who own the roles corresponding priviledges, and deleting certain priviledges will also deprive the corresponding users of the priviledges (unless the users themselves have the priviledges).

#### List of Priviledges Included in the System

<center>**List of Priviledges Included in the System**

|Priviledge Name|Interpretation|
|:---|:---|
|SET\_STORAGE\_GROUP|create timeseries; set storage groups; path dependent|
|INSERT\_TIMESERIES|insert data; path dependent|
|UPDATE\_TIMESERIES|update data; path dependent|
|READ\_TIMESERIES|query data; path dependent|
|DELETE\_TIMESERIES|delete data or timeseries; path dependent|
|CREATE\_USER|create users; path independent|
|DELETE\_USER|delete users; path independent|
|MODIFY\_PASSWORD|modify passwords for all users; path independent; (Those who do not have this priviledge can still change their own asswords. )|
|LIST\_USER|list all users; list a user's priviledges; list a user's roles with three kinds of operation priviledges; path independent|
|GRANT\_USER\_PRIVILEGE|grant user priviledges; path independent|
|REVOKE\_USER\_PRIVILEGE|revoke user priviledges; path independent|
|GRANT\_USER\_ROLE|grant user roles; path independent|
|REVOKE\_USER\_ROLE|revoke user roles; path independent|
|CREATE\_ROLE|create roles; path independent|
|DELETE\_ROLE|delete roles; path independent|
|LIST\_ROLE|list all roles; list the priviledges of a role; list the three kinds of operation priviledges of all users owning a role; path independent|
|GRANT\_ROLE\_PRIVILEGE|grant role priviledges; path independent|
|REVOKE\_ROLE\_PRIVILEGE|revoke role priviledges; path independent|
</center>

#### Username Restrictions
IoTDB specifies that the character length of a username should not be less than 4, and the username cannot contain spaces.
#### Password Restrictions
IoTDB specifies that the character length of a password should not be less than 4, and the password cannot contain spaces. The password is encrypted with MD5.
#### Role Name Restrictions
IoTDB specifies that the character length of a role name should not be less than 4, and the role name cannot contain spaces.
