<!-- TOC -->

- [Have Questions](#have-questions)
    - [Mailing Lists](#mailing-lists)
    - [JIRA issues](#jira-issues)
- [How to contribute](#how-to-contribute)
    - [Becoming a committer](#becoming-a-committer)
        - [Contributing by Helping Other Users](#contributing-by-helping-other-users)
        - [Contributing by Testing Releases](#contributing-by-testing-releases)
        - [Contributing by Reviewing Changes](#contributing-by-reviewing-changes)
        - [Contributing by Documentation Changes](#contributing-by-documentation-changes)
        - [Contributing Bug Reports](#contributing-bug-reports)
        - [Contributing Code Changes](#contributing-code-changes)
            - [Cloning source code](#cloning-source-code)
            - [JIRA](#jira)
            - [Pull Request](#pull-request)
            - [The Review Process](#the-review-process)
            - [Closing Your Pull Request / JIRA](#closing-your-pull-request--jira)
            - [Code Style](#Code-Style)

<!-- /TOC -->

# Have Questions

## Mailing Lists

It is recommmended to use our mail lists to ask for help, debug issues or contributiong to the project.

* dev@iotdb.apache.org is for people who want to contribute codes to IoTDB or have usage questions for IoTDB.

Some quick tips when using email:
* For error logs or long code examples, please use GitHub gist and include only a few lines of the pertinent code / log within the email.
* No jobs, sales, or solicitation is permitted on the Apache IoTDB mailing lists.

PS. To subscribe our mail list, you can send an email to dev-subscribe@iotdb.incubator.apache.org and you will receive a "confirm subscribe to dev@iotdb.apache.org" email, follow the steps to confirm your subscribe.

## JIRA issues

The project tracks issues and new features on [JIRA issues](https://issues.apache.org/jira/projects/IOTDB/issues). You can create a new issue to report a bug, request a new feature or provide your custmon issue.

# How to contribute

## Becoming a committer

To become a committer, you should first be active on our community so that most of our existing committers recognize you. Pushing codes and creating pull requests is just one of committer's rights. Moreover, it is committer's duty to help new uesrs on mail list, test new releases and improve documentation.

### Contributing by Helping Other Users

Since IoTDB always attracts new users, it would be great appreciate if you can help them by answering questions on the dev@iotdb.apache.org mail list. We regard it as a valuable contribution. Also, the more questions you answer, the more poeple know you. Popularity is one of the necessary conditions to be become a committer.

Contributors should subscribe to our mail list to catch up the lates progress.

### Contributing by Testing Releases

IoTDB's new release is visable to everyone, members of community can vote to accpect these releases on the dev@iotdb.apache.org mailing list. Users of IoTDB will be invited to try out on their workloads and provide feedback on any performance or correctness issues found in the newer release.

### Contributing by Reviewing Changes

Changes to IoTDB source codes are made through Github pull request, anyone can review and comment on these changes. Reviewing others' pull requests can help you comprehend how a bug is fixed or a new feature is added. Besides, Learning directly from source codes will give you a deeper understanding of how IoTDB system works and where its bottlenecks lie. You can help by reviewing the changes and asking questions or pointing out issues.

### Contributing by Documentation Changes

To propose a change to release documentation (that is, docs that appear under <https://iotdb.apache.org/#/Documents>), edit the Markdown source files in Iotdb’s docs/ directory(`documentation-EN` branch). The process to propose a doc change is otherwise the same as the process for proposing code changes below.  

### Contributing Bug Reports

If you encounter a problem, you should first confirm the problem you found is a real bug, which means that it is not error outputs or caused by your error configuration or commands. Then try to search mail list to check whehter other people hava faced the same situation.

Once you are sure it is a bug, it may be reported by creating a JIRA but without creating a pull request. In the bug report, you should provide enough information to understand, isolate and ideally reproduce the bug. Unreproducible bugs, or simple error reports, may be closed.

It’s very helpful if the bug report has a description about how the bug was introduced, by which commit, so that reviewers can easily understand the bug. It also helps committers to decide how far the bug fix should be backported, when the pull request is merged. The pull request to fix the bug should narrow down the problem to the root cause.

Performance regression is also one kind of bug. The pull request to fix a performance regression must provide a benchmark to prove the problem is indeed fixed.

Note that, data correctness/data loss bugs are very serious. Make sure the corresponding bug report JIRA ticket is labeled as correctness or data-loss. If the bug report doesn’t get enough attention, please send an email to dev@iotdb.apache.org, to draw more attentions.

### Contributing Code Changes

> When you contribute code, you affirm that the contribution is your original work and that you license the work to the project under the project’s open source license. Whether or not you state this explicitly, by submitting any copyrighted material via pull request, email, or other means you agree to license the material under the project’s open source license and warrant that you have the legal authority to do so.

#### Cloning source code

```
$ git clone git@github.com:apache/incubator-iotdb.git
```
Following README.md to test, run or build IoTDB.

#### JIRA

Generally, IoTDB uses JIRA to track logical issues, including bugs and improvements, and uses Github pull requests to manage the review and merge of specific code changes. That is, JIRAs are used to describe what should be fixed or changed, and high-level approaches, and pull requests describe how to implement that change in the project’s source code. For example, major design decisions are discussed in JIRA.

1. Find the existing IoTDB JIRA that the change pertains to.
    1. Do not create a new JIRA if creating a change to address an existing issue in JIRA; add to the existing discussion and work instead
    2. Look for existing pull requests that are linked from the JIRA, to understand if someone is already working on the JIRA
2. If the change is new, then it usually needs a new JIRA. However, trivial changes, where the what should change is virtually the same as the how it should change do not require a JIRA. Example: Fix spelling error in javadoc
3. If required, create a new JIRA:
    1. Provide a descriptive Title. “Problem in XXXManager” is not sufficient. “IoTDB failed to start on jdk11 because jdk11 does not support -XX:+PrintGCDetail” is good.
    2. Write a detailed Description. For bug reports, this should ideally include a short reproduction of the problem. For new features, it may include a design document.
    3. Set required fields:
        1. Issue Type. Generally, Bug, Improvement and New Feature are the only types used in IoTDB.
        2. Priority. Set to Major or below; higher priorities are generally reserved for committers to set. The main exception is correctness or data-loss issues, which can be flagged as Blockers. JIRA tends to unfortunately conflate “size” and “importance” in its Priority field values. Their meaning is roughly:
            1. Blocker: pointless to release without this change as the release would be unusable to a large minority of users. Correctness and data loss issues should be considered Blockers.
            2. Critical: a large minority of users are missing important functionality without this, and/or a workaround is difficult
            3. Major: a small minority of users are missing important functionality without this, and there is a workaround
            4. Minor: a niche use case is missing some support, but it does not affect usage or is easily worked around
            5. Trivial: a nice-to-have change but unlikely to be any problem in practice otherwise
        3. Affects Version. For Bugs, assign at least one version that is known to exhibit the problem or need the change
        4. Label. Not widely used, except for the following:
            * correctness: a correctness issue
            * data-loss: a data loss issue
            * release-notes: the change’s effects need mention in release notes. The JIRA or pull request should include detail suitable for inclusion in release notes – see “Docs Text” below.
            * starter: small, simple change suitable for new contributors
        5. Docs Text: For issues that require an entry in the release notes, this should contain the information that the release manager should include in Release Notes. This should include a short summary of what behavior is impacted, and detail on what behavior changed. It can be provisionally filled out when the JIRA is opened, but will likely need to be updated with final details when the issue is resolved.
    4. Do not set the following fields:
        1. Fix Version. This is assigned by committers only when resolved.
        2. Target Version. This is assigned by committers to indicate a PR has been accepted for possible fix by the target version.
    5. Do not include a patch file; pull requests are used to propose the actual change.
4. If the change is a large change, consider inviting discussion on the issue at dev@iotdb.apache.org first before proceeding to implement the change.


#### Pull Request

1. Fork the Github repository at https://github.com/apache/incubator-iotdb if you haven’t already
2. Clone your fork, create a new branch, push commits to the branch.
3. Consider whether documentation or tests need to be added or updated as part of the change, and add them as needed.
Run all tests with [How to test](https://github.com/thulab/iotdb/wiki/How-to-test-IoTDB) to verify that the code still compiles, passes tests.
4. Open a pull request against the master branch of IoTDB. (Only in special cases would the PR be opened against other branches.)
    1. The PR title should be of the form "IoTDB-xxxx" Title, where xxxx is the relevant JIRA number.
    2. If the pull request is still a work in progress, and so is not ready to be merged, but needs to be pushed to Github to facilitate review, then add "WIP" after the component.
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
* Lively, polite, rapid technical debate is encouraged from everyone in the community. The outcome may be a rejection of the entire change.
* Keep in mind that changes to more critical parts of IoTDB, like its read/write data from/to disk, will be subjected to more review, and may require more testing and proof of its correctness than other changes.
* Reviewers can indicate that a change looks suitable for merging with a comment such as: “I think this patch looks good” or "LGTM". If you comment LGTM you will be expected to help with bugs or follow-up issues on the patch. Consistent, judicious use of LGTMs is a great way to gain credibility as a reviewer with the broader community.
* Sometimes, other changes will be merged which conflict with your pull request’s changes. The PR can’t be merged until the conflict is resolved. This can be resolved by, for example, adding a remote to keep up with upstream changes by 

```shell
git remote add upstream git@github.com:apache/incubator-iotdb.git
git fetch upstream
git rebase upstream/master 
# resolve your conflicts
# push codes to your branch
```

* Try to be responsive to the discussion rather than let days pass between replies

#### Closing Your Pull Request / JIRA
* If a change is accepted, it will be merged and the pull request will automatically be closed, along with the associated JIRA if any
    * Note that in the rare case you are asked to open a pull request against a branch besides master, that you will actually have to close the pull request manually
    * The JIRA will be Assigned to the primary contributor to the change as a way of giving credit. If the JIRA isn’t closed and/or Assigned promptly, comment on the JIRA.
* If your pull request is ultimately rejected, please close it promptly
    * … because committers can’t close PRs directly
    * Pull requests will be automatically closed by an automated process at Apache after about a week if a committer has made a comment like “mind closing this PR?” This means that the committer is specifically requesting that it be closed.
* If a pull request has gotten little or no attention, consider improving the description or the change itself and ping likely reviewers again after a few days. Consider proposing a change that’s easier to include, like a smaller and/or less invasive change.
* If it has been reviewed but not taken up after weeks, after soliciting review from the most relevant reviewers, or, has met with neutral reactions, the outcome may be considered a “soft no”. It is helpful to withdraw and close the PR in this case.
* If a pull request is closed because it is deemed not the right approach to resolve a JIRA, then leave the JIRA open. However if the review makes it clear that the issue identified in the JIRA is not going to be resolved by any pull request (not a problem, won’t fix) then also resolve the JIRA

#### Code Style

For Java code, Apache IoTDB follows Google’s Java Style Guide.


