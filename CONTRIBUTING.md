# 给 mxbench 做贡献

欢迎来到 MatrixBench 的世界，如果你想为它做贡献，请参考以下的开发者指南。

## 你可以做什么
“毋以善小而不为” —— 我们鼓励任何可以让本项目更好的行动。这在 Github 上可以通过提交 PR(Pull Request) 来实现。

* 如果你观察到了一处拼写错误，别犹豫，请随手提PR帮助我们改正！
* 如果你发现了一个 bug, 就试试修复！
* 如果你找到了一些冗余代码，来帮我们把它移除吧！
* 如果你认为现有测试没能覆盖到某些case，为什么不顺手加上呢？
* 如果你可以提升一个feature, 请 **不要** 犹豫!
* 如果你感觉某段代码非常晦涩难懂，可以加一些注释提升易读性，因为别人也很可能与你有相同的感受。
* 如果你嗅到了代码的“坏气味”，来吧，拿起重构的武器！
* 甚至，如果你觉得文档有一些可以改进的地方，那太好了，改进它！
* 文档若有不正确的地方，你的修复就是我们最渴求的！
* ...

该列表无法穷举你所能作出的潜在贡献。所以只需记住一句话：

**我们期待你的PR！**


## 贡献
### 准备
在你做出贡献之前, 你需要注册一个 Github ID. 你需要安装一下软件:
* go
* git

### Workflow
我们使用 `master` 作为开发分支，这同时也意味着该分支不保证稳定，不建议运用于生产环境。

以下是贡献者的 workflow：

1. Fork 一份 mxbench 仓库到自己的账户下；
2. Clone 一份 fork 出的仓库到本地；
3. 创建一个分支，并在该分支上工作；
4. 因为上游仓库中也可能会有一些更新，所以你时不时需要为你的工作分支做一做同步；
5. Commit 你的修改 （有关 Commit 一些规则，包括 Commit Message 和 Commit 内容等，相见[Commit 规则](#commit-规则)）
6. 将 Commits 推送到你 Fork 出的远程仓库中；
7. 创建一个PR.

创建PR请遵循 Github 官方文档 [Creating a pull request](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request).
请保证每个PR都有一个对应的 issue.

创建 PR 之后，一名或多名 code reviewers 会被分配至此PR进行代码审查。
在 PR 通过代码审查即将 Merge 之际，请选择 `Squash And Merge` 选项， 则该 PR 下的各 Commits 会被 Squash 成一条 Commit。

### 编译
进入该项目的根目录并执行以下命令进行编译:
```bash
make build
```

清理:
```bash
make clean
```

### 代码风格
请参见 [Uber Go Style Guide](https://github.com/uber-go/guide/blob/master/style.md)

### Commit 规则
#### Commit Message

Commit message 可以帮助代码审查者更好地理解提交该PR的目的，并可以加速审查过程。代码贡献者应使用清晰的 commit message, 避免歧义。

以下几条规则可以帮助大家写出一个专业的Commit Message：
1. 使用英文描述
2. Commit message分为标题和正文两部分
3. 标题和正文之间用空行隔开
4. 标题首字母大写
5. 标题尽量使用动词语境，例如Fix xxx, Remove xxx, Enable xxx
6. 标题尽量不超过50个字符，正文每行尽量不超过72字符
7. 正文的描述可以采用下面两种格式之一或者两者结合的格式：
  1. What changed + Why: 阐述这个commit做了哪些改动，阐述为什么要这么改动。
  2. What is the problem + How to resolve: 阐述问题及相关背景，阐述怎么解决这个问题。


一般而言，我们鼓励以下的 commit message 风格。

* feat: 一个新的 feature
* fix: 一个 bug 修复
* docs: 只对文档做出修改
* style: 不改变代码实质内容，只涉及风格
* refactor: 代码重构，即不涉及 bug 修复，又不涉及 feature 开发
* perf: 提升性能的代码改进
* test: 添加测试用例或改进现有测试
* chore: 对一些项目构建过程和辅助工具做一些改动

另一方面，请避免以下 commit message:
* ~~fix bug~~
* ~~update~~
* ~~add doc~~

更多信息可以可以参考 [How to Write a Git Commit Message](http://chris.beams.io/posts/git-commit/).

#### Commit 内容

* 避免非常大的单个 Commit
* 避免在单个 Commit 中杂糅多个主题
* 每个 Commit 要完整且可审查。

### Pull Request
我们使用 [GitHub Issues](https://github.com/ymatrix-data/mxbench/issues) 以及 [Pull Requests](https://github.com/ymatrix-data/mxbench/pulls) 分别记录、追踪 issues 和 PR.

如果你发现文档中的拼写错误或错别字，发现代码bug，或者想提一个新需求或提一些建议，你可以[在 Github 上创建一个新的 issue](https://github.com/ymatrix-data/mxbench/issues/new) 来告知我们。

如果你想做一些贡献，请遵循 [workflow](#Workflow) 描述的步骤进行，以创建一个新的 PR.
如果你的 PR 涉及到比较大规模的改动，例如组件重构或添加一个新的组件，请为它撰写详细的设计文档和使用文档。

之前已经提到，我们要避免过大的单个 PR. 可以考虑把它拆分成若干个 PR 进行提交。

### Code Review
所有需要提交的代码都必须经过一名或多名代码审查者的审查。有一些原则：

- 可读: 重要的代码应该像文档一样可阅读；
- 优雅: 新函数，结构体或组件应该拥有良好的设计；
- 可测试: 重要代码应该拥有高单元测试覆盖率。

## 其他

### 为你的工作署名
规则很简单: 如果你能够保证一下条款 (from [developercertificate.org](http://developercertificate.org/)):

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.
660 York Street, Suite 102,
San Francisco, CA 94110 USA

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.

Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

那么每次只需在 commit message 末尾添加以下一行文字:

```
Signed-off-by: Joe Smith <joe.smith@email.com>
```

使用你的真名。

如果你在 git configs 配置了 `user.name` 以及 `user.email`, 你可以通过  `git commit -s` 自动为你的 commit 署名.

