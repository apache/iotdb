### v0.7.0 Release Notes

Add postback tools, multi-path data storage mechanism, ```SHOW TIMESERIES / STORAGE GROUP``` SQL extended expressions, and other new features. Fix several issues in version 0.6.0. Improve system stability.

#### Features

##### IoTDB

* Add ```Show Storage Group``` SQL statement, support for displaying storage groups.
* Enhance ```Show Timeseries``` SQL, support for displaying time series information under different prefix paths.
* Add multi-path data storage mechanism for distributed storage, allowing different data files (write ahead logs, metadata files, etc.) to be stored in different paths.
* Add data directory configuration which allows data files to be stored in different paths, facilitating the use of multiple disks to store data files.

##### IoTDB-Transfer-Tool

* Added IoTDB data postback module to provide users with end-to-cloud timed data file postback function.

#### Bugfixes
##### IoTDB
* Fix the problem that the IoTDB shutdown script does not work
* Fix the problem that the IoTDB installation path does not support spaces in Windows environment.
* Fix the problem that the system cannot be restarted after merging Overflow data
* Fix the problem that the permission module missing the ALL keyword
* Fixed an issue that quotation marks in strings were not supported when querying TEXT type data

#### System Organization
* Further improve system stability