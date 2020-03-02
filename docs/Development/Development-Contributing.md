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

<!-- TOC -->

## Outline
- Have Questions
    - Mailing Lists
    - JIRA issues
- How to contribute
    - Becoming a committer
        - Contributing by Helping Other Users
        - Contributing by Testing Releases
        - Contributing by Reviewing Changes
        - Contributing by Documentation Changes
        - Contributing Bug Reports
        - Contributing Code Changes
            - Cloning source code
            - JIRA
            - Pull Request
            - The Review Process
            - Closing Your Pull Request / JIRA
            - Code Style

<!-- /TOC -->

# Have Questions

## Mailing Lists

It is recommended to use our mailing lists to ask for help, report issues or contribute to the project.
dev@iotdb.apache.org is for anyone who wants to contribute codes to IoTDB or have usage questions for IoTDB.

Some quick tips when using email:
* For error logs or long code examples, please use GitHub gist and include only a few lines of the pertinent code/log within the email.
* No jobs, sales, or solicitation is permitted on the Apache IoTDB mailing lists.

PS. To subscribe our mail list, you can send an email to dev-subscribe@iotdb.incubator.apache.org and you will receive a "confirm subscribe to dev@iotdb.apache.org" email, following the steps to confirm your subscription.

## JIRA issues

The project tracks issues and new features on [JIRA issues](https://issues.apache.org/jira/projects/IOTDB/issues). You can create a new issue to report a bug, request a new feature or provide your custom issue.

# How to contribute

## Becoming a committer

To become a committer, you should first be active in our community so that most of our existing committers recognize you. Pushing codes and creating pull requests is just one of the committer's rights. Moreover, it is committer's duty to help new users on the mail list, test new releases and improve documentation.

### Contributing by Helping Other Users

Since Apache IoTDB always attracts new users, it would be great if you can help them by answering questions on the dev@iotdb.apache.org mail list. We regard it as a valuable contribution. Also, the more questions you answer, the more people know you. Popularity is one of the necessary conditions to be a committer.

Contributors should subscribe to our mailing list to catch up the latest progress.

### Contributing by Testing Releases

IoTDB's new release is visible to everyone, members of the community can vote to accept these releases on the dev@iotdb.apache.org mailing list. Users of IoTDB will be invited to try out on their workloads and provide feedback on any performance or correctness issues found in the newer release.

### Contributing by Reviewing Changes

Changes to IoTDB source codes are made through Github pull request. Anyone can review and comment on these changes. Reviewing others' pull requests can help you comprehend how a bug is fixed or a new feature is added. Besides, Learning directly from the source code will give you a deeper understanding of how IoTDB system works and where its bottlenecks lie. You can help by reviewing the changes, asking questions and pointing out issues.

### Contributing by Documentation Changes

To propose a change to release documentation (that is, docs that appear under <https://iotdb.apache.org/#/Documents/progress/chap1/sec1>), edit the Markdown source files in IoTDB’s docs/ directory(`documentation-EN` branch). The process to propose a doc change is otherwise the same as the process for proposing code changes below.  

Whenever updating **User Guide** documents, remember to update `0-Content.md` at the same time. Here are two brief examples to show how to add new documents or how to modify existing documents:

1. Suppose we have "chapter 1:Overview" already, and want to add a new document `A.md` in chapter 1.
Then,
   * Step 1: add document named `5-A.md` in folder "1-Overview", since it is the fifth section in this chapter;
   * Step 2: modify `0-Content.md` file by adding `* 5-A.md` in the list of "# Chapter 1: Overview".

2. Suppose we want to create a new chapter "chapter7: RoadMap", and want to add a new document `B.md` in chapter 7.
Then,
   * Step 1: create a new folder named "7-RoadMap", and add document named `1-B.md` in folder "7-RoadMap";
   * Step 2: modify `0-Content.md` file by adding "# Chapter 7: RoadMap" in the end, and  adding `* 1-B.md` in the list of this new chapter.

If you need to insert **figures** into documents, you can firstly update the figures in [this issue](https://github.com/thulab/iotdb/issues/543) for storing pictures in IoTDB website or other MD files.
Drag a picture and then quote the figure's URL link. 

### Contributing Bug Reports

If you encounter a problem, try to search the mailing list and JIRA to check whether other people have faced the same situation. If it is not reported before, please report an issue.

Once you are sure it is a bug, it may be reported by creating a JIRA without creating a pull request. In the bug report, you should provide enough information to understand, isolate and ideally reproduce the bug. Unreproducible bugs, or simple error reports, may be closed.

It’s very helpful if the bug report has a description about how the bug was introduced, by which commit, so that reviewers can easily understand the bug. It also helps committers to decide how far the bug fix should be backported, when the pull request is merged. The pull request to fix the bug should narrow down the problem to the root cause.

Performance regression is also one kind of bug. The pull request to fix a performance regression must provide a benchmark to prove the problem is indeed fixed.

Note that, data correctness/loss bugs are our first priority to solve. Please make sure the corresponding bug-reporting JIRA ticket is labeled as correctness or data-loss. If the bug report doesn’t gain enough attention, please include it and send an email to dev@iotdb.apache.org.

### Contributing Code Changes

> When you contribute code, you affirm that the contribution is your original work and that you license the work to the project under the project’s open-source license. Whether or not you state this explicitly, by submitting any copyrighted material via pull request, email, or other means you agree to license the material under the project’s open-source license and warrant that you have the legal authority to do so.  Any new files contributed should be under Apache 2.0 License with a header on top of it.

#### Cloning source code

```
$ git clone git@github.com:apache/incubator-iotdb.git
```
Following `README.md` to test, run or build IoTDB.

#### JIRA

Generally, IoTDB uses JIRA to track logical issues, including bugs and improvements and uses Github pull requests to manage the review and merge specific code changes. That is, JIRAs are used to describe what should be fixed or changed, proposing high-level approaches. Pull requests describe how to implement that change in the project’s source code. For example, major design decisions discussed in JIRA.

1. Find the existing IoTDB JIRA that the change pertains to.
    1. Do not create a new JIRA if you send a PR to address an existing issue labeled in JIRA; add it to the existing discussion.
    2. Look for existing pull requests that are linked from the JIRA, to understand if someone is already working on the JIRA
2. If the change is new, then it usually needs a new JIRA. However, trivial changes, such as changes are self-explained, do not require a JIRA. Example: Fix spelling error in JavaDoc
3. If required, create a new JIRA:
    1. Provide a descriptive Title. “Problem in XXXManager” is not sufficient. “IoTDB failed to start on jdk11 because jdk11 does not support -XX:+PrintGCDetail” is good.
    2. Write a detailed description. For bug reports, this should ideally include a short reproduction of the problem. For new features, it may include a design document.
    3. Set the required fields:
        1. Issue Type. Generally, Bug, Improvement and New Feature are the only types used in IoTDB.
        2. Priority. Set to Major or below; higher priorities are generally reserved for committers to set. The main exception is correctness or data-loss issues, which can be flagged as Blockers. JIRA tends to unfortunately conflate “size” and “importance” in its Priority field values. Their meaning is rough:
            1. Blocker: pointless to release without this change as the release would be unusable to a large minority of users. Correctness and data loss issues should be considered Blockers.
            2. Critical: a large minority of users are missing important functionality without this, and/or a workaround is difficult
            3. Major: a small minority of users are missing important functionality without this, and there is a workaround
            4. Minor: a niche use case is missing some support, but it does not affect usage or is easily worked around
            5. Trivial: a nice-to-have change but unlikely to be any problem in practice otherwise
        3. Affected Version. For Bugs, assign at least one version that is known to reproduce the issue or need to be changed
        4. Label. Not widely used, except for the following:
            * correctness: a correctness issue
            * data-loss: a data loss issue
            * release-notes: the change’s effects need mention in release notes. The JIRA or pull request should include detail suitable for inclusion in release notes – see “Docs Text” below.
            * starter: small, simple change suitable for new contributors
        5. Docs Text: For issues that require an entry in the release notes, this should contain the information that the release manager should include. Issues should include a short summary of what behavior is impacted, and detail on what behavior changed. It can be provisionally filled out when the JIRA is opened, but will likely need to be updated with final details when the issue is resolved.
    4. Do not set the following fields:
        1. Fix Version. This is assigned by committers only when resolved.
        2. Target Version. This is assigned by committers to indicate a PR has been accepted for possible fix by the target version.
    5. Do not include a patch file; pull requests are used to propose the actual change.
4. If the change is a large change, consider raising a discussion on it at dev@iotdb.apache.org first before proceeding to implement the change. Currently, we use https://cwiki.apache.org/confluence/display/IOTDB to store design proposals and release process. Users can also send them there.


#### Pull Request

1. Fork the Github repository at https://github.com/apache/incubator-iotdb if you haven’t done already.
2. Clone your fork, create a new branch, push commits to the branch.
3. Please add documentation and tests to explain/cover your changes.
Run all tests with [How to test](https://github.com/thulab/iotdb/wiki/How-to-test-IoTDB) to verify your change.
4. Open a pull request against the master branch of IoTDB. (Only in special cases would the PR be opened against other branches.)
    1. The PR title should be in the form of "IoTDB-xxxx", where xxxx is the relevant JIRA number.
    2. If the pull request is still under work in progress stage but needs to be pushed to Github to request for review, please add "WIP" after the PR title.
    3. Consider identifying committers or other contributors who have worked on the code being changed. Find the file(s) in Github and click “Blame” to see a line-by-line annotation of who changed the code last. You can add @username in the PR description to ping them immediately.
    4. Please state that the contribution is your original work and that you license the work to the project under the project’s open source license.
5. The related JIRA, if any, will be marked as “In Progress” and your pull request will automatically be linked to it. There is no need to be the Assignee of the JIRA to work on it, though you are welcome to comment that you have begun work.
6. The Jenkins automatic pull request builder will test your changes
    1. If it is your first contribution, Jenkins will wait for confirmation before building your code and post “Can one of the admins verify this patch?”
    2. A committer can authorize testing with a comment like “ok to test”
    3. A committer can automatically allow future pull requests from a contributor to be tested with a comment like “Jenkins, add to whitelist”
7. Watch for the results, and investigate and fix failures promptly
    1. Fixes can simply be pushed to the same branch from which you opened your pull request
    2. Jenkins will automatically re-test when new commits are pushed
    3. If the tests failed for reasons unrelated to the change (e.g. Jenkins outage), then a committer can request a re-test with “Jenkins, retest this please”. Ask if you need a test restarted. If you were added by “Jenkins, add to whitelist” from a committer before, you can also request the re-test.

#### The Review Process

* Other reviewers, including committers, may comment on the changes and suggest modifications. Changes can be added by simply pushing more commits to the same branch.
* Lively, polite, rapid technical debate is encouraged by everyone in the community. The outcome may be a rejection of the entire change.
* Keep in mind that changes to more critical parts of IoTDB, like its read/write data from/to disk, will be subjected to more review, and may require more testing and proof of its correctness than other changes.
* Reviewers can indicate that a change looks suitable for merging with a comment such as: “I think this patch looks good” or "LGTM". If you comment LGTM, you will be expected to help with bugs or follow-up issues on the patch. Consistent, judicious use of LGTMs is a great way to gain credibility as a reviewer with the broader community.
* Sometimes, other changes will be merged which conflict with your pull request’s changes. The PR can’t be merged until the conflict is resolved. This can be resolved by, for example, adding a remote to keep up with upstream changes by 

```shell
git remote add upstream git@github.com:apache/incubator-iotdb.git
git fetch upstream
git rebase upstream/master 
# or you can use `git pull --rebase upstream master` to replace the above two commands
# resolve your conflicts
# push codes to your branch
```

* Try to be responsive to the discussion rather than let days pass between replies

#### Closing Your Pull Request / JIRA
* If a change is accepted, it will be merged, and the pull request will automatically be closed, along with the associated JIRA if any
    * Note that in the rare case you are asked to open a pull request against a branch beside the master, you actually have to close the pull request manually
    * The JIRA will be Assigned to the primary contributor to the change as a way of giving credit. If the JIRA isn’t closed and/or Assigned promptly, comment on the JIRA.
* If your pull request is ultimately rejected, please close it promptly
    * … because committers can’t close PRs directly
    * Pull requests will be automatically closed by an automated process at Apache after about a week if a committer has made a comment like “mind closing this PR?” This means that the committer is specifically requesting that it be closed.
* If a pull request has gotten little or no attention, consider improving the description or the change itself and ping likely reviewers again after a few days. Consider proposing a change that’s easier to include, like a smaller and/or less invasive change.
* If it has been reviewed but not taken up after weeks, after soliciting review from the most relevant reviewers, or, has met with neutral reactions, the outcome may be considered a “soft no”. It is helpful to withdraw and close the PR in this case.
* If a pull request is closed because it is deemed not the right approach to resolve a JIRA, then leave the JIRA open. However, if the review makes it clear that the issue identified in the JIRA is not going to be resolved by any pull request (not a problem, won’t fix) then also resolve the JIRA

#### Code Style

For Java code, Apache IoTDB follows Google’s Java Style Guide.

#### Unit Test

When writing unit tests, note the path to generate the test file at test time, which we require to be generated in the `target` directory and placed under the `constant` package for each test project