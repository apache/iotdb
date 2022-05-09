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

# Howto Submit Code

## Contribution

IoTDB invites developers to participate in the construction of open source projects

You can check [issues](https://issues.apache.org/jira/projects/IOTDB/issues) and participate in the resolution, or make other improvements.

After submitting the pr, after passing the Travis-CI test and Sonar code quality inspection, at least one Committer agrees and the code does not conflict, you can merge

## PR guide

You can easily submit [Pull Request (PR)](https://help.github.com/articles/about-pull-requests/) on Github, the following will use this website project [apache/iotdb](https://github.com/apache/iotdb) as an example (if it is another project, please replace the project name iotdb)

### Fork repository

Visit the apache/iotdb project’s [github page](https://github.com/apache/iotdb), click `Fork` button on the right left cornor.

![](https://user-images.githubusercontent.com/37333508/79351839-bd288900-7f6b-11ea-8d12-feb18c35adad.png)

### Setup local repository

- Clone the source code to local machine:

```
git clone https://github.com/<your_github_name>/iotdb.git
```

**Note: substitute <your_github_name> with your github username.**

After the clone is done, the origin remote will point to the default branch of the cloned repository.

- add apache/iotdb as upstream remote:

```
cd  iotdb
git remote add upstream https://github.com/apache/iotdb.git
```

- Check the local repository’s remotes

```
git remote -v
origin https://github.com/<your_github_name>/iotdb.git (fetch)
origin    https://github.com/<your_github_name>/iotdb.git(push)
upstream  https://github.com/apache/iotdb.git (fetch)
upstream  https://github.com/apache/iotdb.git (push)
```

- Create a new branch to start working：(e.g. fix)

```
git checkout -b fix
```

You can make code changes after creation.

- Push the changes to a remote repository：(e.g. fix)

```
git commit -a -m "<you_commit_message>"
git push origin fix
```

For more on git usages, please visit[Git tutorial](https://www.atlassian.com/git/tutorials/setting-up-a-repository).

### Submission Considerations

When submitting code on git, you should pay attention to:

- Keep the repository clean:

    - Do not submit binary files, so that the size of the repository only increases due to changes in the code.

    - Do not submit generated code.

- The log should have meaning:

    - Title is jira numbered: [IOTDB-jira number]

    - Title is the issue number of GitHub: [ISSUE-issue number]

        - Write #XXXX in the content for association.

### Create PR

Goto your github page, find the apache/servicecomb-website project, swich to the branch you just pushed, click on `New pull request` and then `Create pull request`, see the image below:If you solve the [issues](https://issues.apache.org/jira/projects/IOTDB/issues), you need to add [IOTDB-xxx] at the beginning，see the image below:

![](https://user-images.githubusercontent.com/37333508/79414865-5f815480-7fde-11ea-800c-47c7dbad7648.png)

Congrautulations, now you have succesfully submitted a PR. For more on PR, please read [collaborating-with-issues-and-pull-requests](https://help.github.com/categories/collaborating-with-issues-and-pull-requests/) 

### Resolve conflicts

When a same piece of file is edited by multiple person simultaneously, conflicts can occur. It can be resolved as follow:

1：Switch to the master branch

```
git checkout master
```

2：Pull the upstream’s master branch

```
git pull upstream master
```

3：Switch back to the branch we are working on(e.g. fix)

```
git checkout fix
```

4：Rebase the working branch onto the master branch

```
git rebase -i master
```

A list of commits will be listed on your text editor. Normally we can just save and exit. Git will now apply the commits one by one onto the master branch until it encounters a conflict. When this happens, the rebase process is paused. We need to resolve the conflicts, then execute

```
git add .
git rebase --continue
```

Repeat this process until all commits are successfully applied. And finally run

5：to push the resolved branch to remote origin

```
git push -f origin fix
```

The code of conduct is derived from[Apache ServiceComb](http://servicecomb.apache.org/developers/submit-codes/)
