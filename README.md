
## 介绍
[![Build Status](https://www.travis-ci.org/openmessaging/dledger.svg?branch=master)](https://www.travis-ci.org/search/dledger) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.openmessaging.storage/dledger/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Copenmessaging-storage-dledger)  [![Coverage Status](https://coveralls.io/repos/github/openmessaging/openmessaging-storage-dledger/badge.svg?branch=master)](https://coveralls.io/github/openmessaging/openmessaging-storage-dledger?branch=master) [![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

一个基于raft的java库，用于构建高可用性、高持久性、强一致性的提交日志，它可以作为分布式存储系统的持久层，例如消息传递、流、kv、db等。

Dledger增加了许多在[原始论文]中没有描述的新功能(https:raft.github.ioraft.pdf)。它已被证明是一个真正的生产准备产品。


## 特性

* Leader选举
* Preferred leader election
* [Pre-vote protocol](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
* 高性能、高可靠的存储支持
* 领导者和追随者之间的并行日志复制
* 异步复制
* 状态机
* Multi-Raft
* High tolerance of symmetric network partition
* High tolerance of asymmetric network partition
* [Jepsen verification with fault injection](https://github.com/openmessaging/openmessaging-dledger-jepsen)

## 源码分析
- DLedgerConfig：主从切换模块相关的配置信息。
- MemberState：节点状态机，即Raft协议中Follower、 Candidate、Leader三种状态的状态机实现。
- DLedgerClientProtocol：DLedger客户端协议，主要定义如下3个方法：
  - CompletableFuture<GetEntriesResponse> get(GetEntriesRequest request)：客户端从服务器获取日志条目（获取数据）。
  - CompletableFuture<AppendEntryResponse> append(AppendEntryRequest request)：客户端向服务器追加日志（存储数据）。
  - CompletableFuture<MetadataResponse> metadata(MetadataRequest request)：获取元数据。
- DLedgerProtocol：DLedger客户端协议，主要定义如下4个方法：
  - CompletableFuture<VoteResponse> vote(VoteRequest request)：发起投票请求。 
  - CompletableFuture<HeartBeatResponse> heartBeat(HeartBeatRequest request)：Leader节点向从节点发送心跳包。 
  - CompletableFuture<PullEntriesResponse> pull(PullEntriesRequest request)：拉取日志条目。 
  - CompletableFuture<PushEntryResponse> push(PushEntryRequest request)：推送日志条目，用于日志传播。
- DLedgerClientProtocolHandler：DLedger客户端协议处理器。
- DLedgerProtocolHander：DLedger服务端协议处理器。
- DLedgerRpcService：DLedger节点之前的网络通信，默认基于Netty实现，默认实现类为DLedgerRpcNettyService。
- DLedgerLeaderElector：基于Raft协议的Leader选举类。（重点，入口：io.openmessaging.storage.dledger.DLedgerLeaderElector.StateMaintainer.doWork#）
- DLedgerServer：基于Raft协议的集群内节点的封装类。
- DLedgerEntryPusher：基于Raft协议的日志复制实现类。
  - EntryDispatcher：Leader节点用于向Follower节点主动同步数据的线程实现，对于一个Raft集群，有多少个Follower节点，在Leader节点中就会有多少个EntryDispatcher线程，每一个线程专门负责向一个Follower节点同步数据。
  - EntryHandler：Follower节点用于处理Leader节点发起的日志复制请求。

## 快速开始

### 准备

* 64bit JDK 1.8+

* Maven 3.2.x

### 如何构建

```
mvn clean install -DskipTests
```

### Run Command Line

#### Help

> Print Help in Command Line

```shell
java -jar example/target/dledger-example.jar
```

#### Appender

**A high-available, high-durable, strong-consistent, append-only log store.**

> Start a Standalone Appender Server

```shell
java -jar example/target/dledger-example.jar appender
```

> Append Data to Appender

```shell
java -jar example/target/dledger-example.jar append -d "Hello World"
```
After this command, you have appended a log which contains "Hello World" to the appender.

> Get Data from Appender

```shell
java -jar example/target/dledger-example.jar get -i 0
```
After this command, you have got the log which contains "Hello World" from the appender.

#### RegisterModel

**A simple multi-register model**

> Start a Standalone RegisterModel Server

```shell
java -jar example/target/dledger-example.jar register
```

> Write Value for a Key

```shell
java -jar example/target/dledger-example.jar write -k 13 -v 31
```

After this command, you have written a key-value pair which is <13, 31> to the register model.

> Read Value for a Key

```shell
java -jar example/target/dledger-example.jar read -k 13
```

After this command, you have read the value 31 for the key 13 from the register model.

## Contributing
We always welcome new contributions, whether for trivial cleanups, big new features. We are always interested in adding new contributors. What we look for are series of contributions, good taste and ongoing interest in the project. If you are interested in becoming a committer, please let one of the existing committers know and they can help you walk through the process.

## License
[Apache License, Version 2.0](https://github.com/openmessaging/openmessaging-storage-dledger/blob/master/LICENSE) Copyright (C) Apache Software Foundation
 
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fopenmessaging%2Fopenmessaging-storage-dledger.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Fopenmessaging%2Fopenmessaging-storage-dledger?ref=badge_large)









