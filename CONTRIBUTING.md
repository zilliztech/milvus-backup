# Contributing to Milvus-Backup

Contributions to Milvus-Backup are welcome from everyone. We strive to make the contribution process simple and straightforward.

The following are a set of guidelines for contributing to Milvus. Following these guidelines makes contributing to this project easy and transparent. These are mostly guidelines, not rules. Use your best judgment, and feel free to propose changes to this document in a pull request.

**Content**

- [Contributing to Milvus-backup](#contributing-to-milvus-backup)
    - [How can you contribute?](#how-can-you-contribute)
        - [Contributing code](#contributing-code)
        - [GitHub workflow](#github-workflow)
        - [General guidelines](#general-guidelines)
        - [Developer Certificate of Origin (DCO)](#developer-certificate-of-origin-dco)
    - [Coding Style](#coding-style)
        - [Golang coding style](#golang-coding-style)
    - [Run unit test with code coverage](#run-unit-test-with-code-coverage)
        - [Golang](#run-golang-unit-tests)
    - [Commits and PRs](#commits-and-prs)


## How can you contribute?

### Contributing code

**If you encounter a bug, you can**

- (**Recommended**) File an issue about the bug.
- Provide clear and concrete ways/scripts to reproduce the bug.
- Provide possible solutions for the bug.
- Pull a request to fix the bug.

**If you're interested in existing issues, you can**

- (**Recommended**) Provide answers for issue labeled `question`.
- Provide help for issues labeled `bug`, `improvement`, and `enhancement` by
    - (**Recommended**) Ask questions, reproduce the issue, or provide solutions.
    - Pull a request to fix the issue.

**If you require a new feature or major enhancement, you can**

- (**Recommended**) File an issue about the feature/enhancement with reasons.
- Provide an MEP for the feature/enhancement.
- Pull a request to implement the MEP.

If you want to become a contributor of Milvus-Backup, submit your pull requests! For those just getting started, see [GitHub workflow](#github-workflow) below.

All submissions will be reviewed as quickly as possible.
There will be a reviewer to review the codes, and an approver to review everything aside the codes.
If everything is perfect, the reviewer will label `/lgtm`, and the approver will label `/approve`.
Once the 2 labels are on your PR, and all actions pass, your PR will be merged into base branch automatically by our @sre-ci-robot

### GitHub workflow

Generally, we follow the "fork-and-pull" Git workflow.

1.  [Fork](https://docs.github.com/en/github/getting-started-with-github/fork-a-repo) the repository on GitHub.
2.  Clone your fork to your local machine with `git clone git@github.com:<yourname>/milvus-backup.git`.
3.  Create a branch with `git checkout -b my-topic-branch`.
4.  [Commit](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/committing-changes-to-a-pull-request-branch-created-from-a-fork) changes to your own branch, then push to GitHub with `git push origin my-topic-branch`.
5.  Submit a [pull request](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/about-pull-requests) so that we can review your changes.

![](docs/developer_guides/figs/fork-and-pull.png)

Remember to [sync your forked repository](https://docs.github.com/en/github/getting-started-with-github/fork-a-repo#keep-your-fork-synced) _before_ submitting proposed changes upstream. If you have an existing local repository, please update it before you start, to minimize the chance of merge conflicts.

```shell
git remote add upstream git@github.com:zilliztech/milvus-backup.git
git fetch upstream
git checkout upstream/master -b my-topic-branch
```

![](docs/developer_guides/figs/local-develop-steps.png)

### General guidelines

Before submitting your pull requests for review, make sure that your changes are consistent with the [coding style](CONTRIBUTING.md#coding-style), and run [unit tests](CONTRIBUTING.md#run-unit-test-with-code-coverage) to check your code coverage rate.

- Include unit tests when you contribute new features, as they help to prove that your code works correctly, and also guard against future breaking changes to lower the maintenance cost.
- Bug fixes also require unit tests, because the presence of bugs usually indicates insufficient test coverage.
- Keep API compatibility in mind when you change code in Milvus-Backup. Reviewers of your pull request will comment on any API compatibility issues.
- When you contribute a new feature to Milvus-Backup, the maintenance burden is (by default) transferred to the Milvus-Backup team. This means that the benefit of the contribution must be compared against the cost of maintaining the feature.

### Developer Certificate of Origin (DCO)

All contributions to this project must be accompanied by acknowledgment of, and agreement to, the [Developer Certificate of Origin](https://developercertificate.org/). Acknowledgment of and agreement to the Developer Certificate of Origin _must_ be included in the comment section of each contribution and _must_ take the form of `Signed-off-by: {{Full Name}} <{{email address}}>` (without the `{}`). Contributions without this acknowledgment will be required to add it before being accepted. If contributors are unable or unwilling to agree to the Developer Certificate of Origin, their contribution will not be included.

Contributors sign-off that they adhere to DCO by adding the following Signed-off-by line to commit messages:

```text
This is my commit message

Signed-off-by: Random J Developer <random@developer.example.org>
```

Git also has a `-s` command line option to append this automatically to your commit message:

```shell
$ git commit -s -m 'This is my commit message'
```

## Coding Style

Keeping a consistent style for code, code comments, commit messages, and PR descriptions will greatly accelerate your PR review process.
We highly recommend you refer to and comply to the following style guides when you put together your pull requests:

### Golang coding style

- Coding style: refer to the [Effictive Go Style Guide](https://golang.org/doc/effective_go) and [Uber Go Style Guide](https://github.com/uber-go/guide/blob/master/style.md)

To format code

```shell
$ make fmt
```

## Run unit test with code coverage

Before submitting your Pull Request, make sure you have run unit test, and your code coverage rate is >= 90%.

### Run golang unit tests

You can run all the Golang unit tests using make.

```shell
$ make test
```

### Using mockery

It is recommended to use [Mockery](https://github.com/vektra/mockery) to generate mock implementations for unit test dependencies.

If your PR changes any interface definition, you shall run following commands to update all mockery implemented type before submitting it:

```shell
make gen-mock
```

## Commits and PRs

- Commit message and PR description style: refer to [good commit messages](https://chris.beams.io/posts/git-commit)