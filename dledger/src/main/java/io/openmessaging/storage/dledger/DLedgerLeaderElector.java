/*
 * Copyright 2017-2022 The DLedger Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger;

import com.alibaba.fastjson.JSON;
import io.openmessaging.storage.dledger.common.ShutdownAbleThread;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.HeartBeatRequest;
import io.openmessaging.storage.dledger.protocol.HeartBeatResponse;
import io.openmessaging.storage.dledger.protocol.LeadershipTransferRequest;
import io.openmessaging.storage.dledger.protocol.LeadershipTransferResponse;
import io.openmessaging.storage.dledger.protocol.VoteRequest;
import io.openmessaging.storage.dledger.protocol.VoteResponse;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于Raft协议的Leader选举类
 */
public class DLedgerLeaderElector {

    private static final Logger LOGGER = LoggerFactory.getLogger(DLedgerLeaderElector.class);

    /**
     * 随机数生成器，对应Raft协议中选举超时时间，是一个随机数
     */
    private final Random random = new Random();
    /**
     * 配置参数
     */
    private final DLedgerConfig dLedgerConfig;
    /**
     * 节点状态机
     */
    private final MemberState memberState;
    /**
     * RPC服务，实现向集群内的节点发送心跳包、投票的RPC。默认是基于Netty实现的：DLedgerRpcNettyService
     */
    private final DLedgerRpcService dLedgerRpcService;

    //as a server handler
    //record the last leader state
    /**
     * 上次收到心跳包的时间戳
     */
    private volatile long lastLeaderHeartBeatTime = -1;
    /**
     * 上次发送心跳包的时间戳
     */
    private volatile long lastSendHeartBeatTime = -1;
    /**
     * 上次成功收到心跳包的时间戳
     */
    private volatile long lastSuccHeartBeatTime = -1;
    /**
     * 一个心跳包的周期，默认为2s
     */
    private int heartBeatTimeIntervalMs = 2000;
    /**
     * 允许最大的n个心跳周期内未收到心跳包，状态为Follower的节点只有超过maxHeartBeatLeak *
     * heartBeatTimeIntervalMs的时间内未收到主节点的心跳包，才会重新
     * 进入Candidate状态，进行下一轮选举。
     */
    private int maxHeartBeatLeak = 3;
    //as a client
    /**
     * 下一次可发起投票的时间，如果当前时间小于该值，说明计时器未过期，此时无须发起投票
     */
    private long nextTimeToRequestVote = -1;
    /**
     * 是否应该立即发起投票。
     * 如果为true，则忽略计时器，该值默认为false。作用是在从节点
     * 收到主节点的心跳包，并且当前状态机的轮次大于主节点轮次（说明
     * 集群中Leader的投票轮次小于从节点的轮次）时，立即发起新的投票
     * 请求
     */
    private volatile boolean needIncreaseTermImmediately = false;
    /**
     * 最小的发送投票间隔时间，默认为300ms
     */
    private int minVoteIntervalMs = 300;
    /**
     * 最大的发送投票间隔时间，默认为1000ms。
     */
    private int maxVoteIntervalMs = 1000;
    /**
     * 注册的节点状态处理器，通过addRoleChangeHandler方法添加
     */
    private final List<RoleChangeHandler> roleChangeHandlers = new ArrayList<>();

    private VoteResponse.ParseResult lastParseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
    /**
     * 上一次投票的开销
     */
    private long lastVoteCost = 0L;
    /**
     * 状态机管理器
     */
    private final StateMaintainer stateMaintainer;

    private final TakeLeadershipTask takeLeadershipTask = new TakeLeadershipTask();

    public DLedgerLeaderElector(DLedgerConfig dLedgerConfig, MemberState memberState,
                                DLedgerRpcService dLedgerRpcService) {
        this.dLedgerConfig = dLedgerConfig;
        this.memberState = memberState;
        this.dLedgerRpcService = dLedgerRpcService;
        this.stateMaintainer = new StateMaintainer("StateMaintainer-" + this.memberState.getSelfId(), LOGGER);
        refreshIntervals(dLedgerConfig);
    }

    public void startup() {
        /**
         * stateMaintainer是Leader选举内部维护的状态机，即维护节
         * 点状态在Follower、Candidate、Leader之间转换，需要先调用其
         * start()方法启动状态机。
         */
        stateMaintainer.start();
        for (RoleChangeHandler roleChangeHandler : roleChangeHandlers) {
            // 依次启动注册的角色转换监听器，即内部状态机的状态发生变更后的事件监听器，是Leader选举的功能扩展点
            roleChangeHandler.startup();
        }
    }

    public void shutdown() {
        stateMaintainer.shutdown();
        for (RoleChangeHandler roleChangeHandler : roleChangeHandlers) {
            roleChangeHandler.shutdown();
        }
    }

    private void refreshIntervals(DLedgerConfig dLedgerConfig) {
        this.heartBeatTimeIntervalMs = dLedgerConfig.getHeartBeatTimeIntervalMs();
        this.maxHeartBeatLeak = dLedgerConfig.getMaxHeartBeatLeak();
        this.minVoteIntervalMs = dLedgerConfig.getMinVoteIntervalMs();
        this.maxVoteIntervalMs = dLedgerConfig.getMaxVoteIntervalMs();
    }

    /**
     * 该方法时从节点在收到主节点心跳包后的响应逻辑
     * @param request
     * @return
     * @throws Exception
     */
    public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception {

        if (!memberState.isPeerMember(request.getLeaderId())) {
            LOGGER.warn("[BUG] [HandleHeartBeat] remoteId={} is an unknown member", request.getLeaderId());
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.UNKNOWN_MEMBER.getCode()));
        }

        if (memberState.getSelfId().equals(request.getLeaderId())) {
            LOGGER.warn("[BUG] [HandleHeartBeat] selfId={} but remoteId={}", memberState.getSelfId(), request.getLeaderId());
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.UNEXPECTED_MEMBER.getCode()));
        }

        if (request.getTerm() < memberState.currTerm()) {
            // 如果Leader节点发出的心跳的任期小于当前节点的任期，则返回EXPIRED_TERM，这样主节点会立即变成Candidate状态
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.EXPIRED_TERM.getCode()));
        } else if (request.getTerm() == memberState.currTerm()) {
            if (request.getLeaderId().equals(memberState.getLeaderId())) {
                // 如果Leader发出的心跳任期和自己的任期相同，则更新lastLeaderHeartBeatTime，表示收到心跳包，并更新lastLeaderHeartBeatTime
                lastLeaderHeartBeatTime = System.currentTimeMillis();
                return CompletableFuture.completedFuture(new HeartBeatResponse());
            }
        }

        //abnormal case
        //hold the lock to get the latest term and leaderId
        synchronized (memberState) {
            if (request.getTerm() < memberState.currTerm()) {
                // 再一次判断一次，防止在第一次判断后，节点状态发生了变化
                // 如果Leader节点发出的心跳的任期小于当前节点的任期，则返回EXPIRED_TERM，这样主节点会立即变成Candidate状态
                return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.EXPIRED_TERM.getCode()));
            } else if (request.getTerm() == memberState.currTerm()) {
                if (memberState.getLeaderId() == null) {
                    // 当前节点还不知道谁是Leader时，收到心跳包，则将leader节点设置为该心跳发送的节点
                    changeRoleToFollower(request.getTerm(), request.getLeaderId());
                    return CompletableFuture.completedFuture(new HeartBeatResponse());
                } else if (request.getLeaderId().equals(memberState.getLeaderId())) {
                    //  如果Leader发出的心跳任期和自己的任期相同，则更新lastLeaderHeartBeatTime，表示收到心跳包，并更新lastLeaderHeartBeatTime
                    lastLeaderHeartBeatTime = System.currentTimeMillis();
                    return CompletableFuture.completedFuture(new HeartBeatResponse());
                } else {
                    // 心跳发送的LeaderId和当前节点LeaderId并不一致，则返回INCONSISTENT_LEADER，这样主节点会立即变成Candidate状态
                    //this should not happen, but if happened
                    LOGGER.error("[{}][BUG] currTerm {} has leader {}, but received leader {}", memberState.getSelfId(), memberState.currTerm(), memberState.getLeaderId(), request.getLeaderId());
                    return CompletableFuture.completedFuture(new HeartBeatResponse().code(DLedgerResponseCode.INCONSISTENT_LEADER.getCode()));
                }
            } else {
                // 如果心跳中的任期大于当前节点的任期，则将自己的状态更改为Candidate，并进入新的任期选举状态，
                // 并返回TERM_NOT_READY，这样主节点可能会立即再发一次心跳
                changeRoleToCandidate(request.getTerm());
                needIncreaseTermImmediately = true;
                //TOOD notify
                return CompletableFuture.completedFuture(new HeartBeatResponse().code(DLedgerResponseCode.TERM_NOT_READY.getCode()));
            }
        }
    }

    /**
     * 将节点角色更改为Leader
     * @param term
     */
    public void changeRoleToLeader(long term) {
        synchronized (memberState) {
            if (memberState.currTerm() == term) {
                memberState.changeToLeader(term);
                lastSendHeartBeatTime = -1;
                // 执行节点变换扩展点代码
                handleRoleChange(term, MemberState.Role.LEADER);
                LOGGER.info("[{}] [ChangeRoleToLeader] from term: {} and currTerm: {}", memberState.getSelfId(), term, memberState.currTerm());
            } else {
                LOGGER.warn("[{}] skip to be the leader in term: {}, but currTerm is: {}", memberState.getSelfId(), term, memberState.currTerm());
            }
        }
    }

    public void changeRoleToCandidate(long term) {
        synchronized (memberState) {
            if (term >= memberState.currTerm()) {
                memberState.changeToCandidate(term);
                handleRoleChange(term, MemberState.Role.CANDIDATE);
                LOGGER.info("[{}] [ChangeRoleToCandidate] from term: {} and currTerm: {}", memberState.getSelfId(), term, memberState.currTerm());
            } else {
                LOGGER.info("[{}] skip to be candidate in term: {}, but currTerm: {}", memberState.getSelfId(), term, memberState.currTerm());
            }
        }
    }

    //just for test
    public void testRevote(long term) {
        changeRoleToCandidate(term);
        lastParseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
        nextTimeToRequestVote = -1;
    }

    public void changeRoleToFollower(long term, String leaderId) {
        LOGGER.info("[{}][ChangeRoleToFollower] from term: {} leaderId: {} and currTerm: {}", memberState.getSelfId(), term, leaderId, memberState.currTerm());
        lastParseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
        memberState.changeToFollower(term, leaderId);
        lastLeaderHeartBeatTime = System.currentTimeMillis();
        handleRoleChange(term, MemberState.Role.FOLLOWER);
    }

    /**
     * 节点处理其他节点发送过来的投票请求。因为一个节点可能会收到多个节点的拉票请求，防止并发问题需要引入锁机制
     * @param request
     * @param self 是否是自己给自己拉票
     * @return
     */
    public CompletableFuture<VoteResponse> handleVote(VoteRequest request, boolean self) {
        //hold the lock to get the latest term, leaderId, ledgerEndIndex
        synchronized (memberState) {
            if (!memberState.isPeerMember(request.getLeaderId())) {
                // 如果拉票的节点不是集群已知的成员，则直接拒绝拉票
                LOGGER.warn("[BUG] [HandleVote] remoteId={} is an unknown member", request.getLeaderId());
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_UNKNOWN_LEADER));
            }
            if (!self && memberState.getSelfId().equals(request.getLeaderId())) {
                // 如果不是自己给自己拉票，但是拉票节点的ID和自己又一致，则直接拒绝拉票。（异常情况，配置有误，才会走入此分支）
                LOGGER.warn("[BUG] [HandleVote] selfId={} but remoteId={}", memberState.getSelfId(), request.getLeaderId());
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_UNEXPECTED_LEADER));
            }

            if (request.getLedgerEndTerm() < memberState.getLedgerEndTerm()) {
                // 如果拉票节点的ledgerEndTerm小于当前节点的ledgerEndTerm，则直接拒绝拉票。
                // 原因是发起投票节点的日志复制进度比当前节点低，这种情况是不能成为主节点的，否则会造成数据丢失。
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_EXPIRED_LEDGER_TERM));
            } else if (request.getLedgerEndTerm() == memberState.getLedgerEndTerm() && request.getLedgerEndIndex() < memberState.getLedgerEndIndex()) {
                // 如果拉票节点的ledgerEndTerm等于当前节点的ledgerEndTerm，但是ledgerEndIndex小于当前节点的ledgerEndIndex，则直接拒绝拉票
                // 原因同样是发起投票节点的日志复制进度比当前节点低，这种情况是不能成为主节点的，否则会造成数据丢失。
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_SMALL_LEDGER_END_INDEX));
            }

            if (request.getTerm() < memberState.currTerm()) {
                // 发起投票节点的投票轮次小于当前节点的投票轮次：投拒绝票，也就是说在Raft协议中，term越大，越有话语权。
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_EXPIRED_VOTE_TERM));
            } else if (request.getTerm() == memberState.currTerm()) {
                // 发起投票节点的投票轮次等于当前节点的投票轮次：说明两者都处在同一个投票轮次中，地位平等，接下来看该节点是否已经投过票。
                if (memberState.currVoteFor() == null) {
                    // 当前还未投票
                } else if (memberState.currVoteFor().equals(request.getLeaderId())) {
                    // 当前已经投过该节点了
                } else {
                    if (memberState.getLeaderId() != null) {
                        // 如果该节点已存在Leader节点，则拒绝并告知已存在Leader节点
                        return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_ALREADY_HAS_LEADER));
                    } else {
                        // 如果该节点还未有Leader节，如果发起投票节点的投票轮次小于ledgerEndTerm，则以同样
                        //的理由拒绝点，但已经投了其他节点的票，则拒绝请求节点，并告知已投票。
                        return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_ALREADY_VOTED));
                    }
                }
            } else {
                // 发起投票节点的投票轮次大于当前节点的投票轮次：拒绝发起投票节点的投票请求，并告知对方自己还未准备投票，会使用发起投票节点的投票轮次立即进入Candidate状态。
                //stepped down by larger term
                changeRoleToCandidate(request.getTerm());
                needIncreaseTermImmediately = true;
                //only can handleVote when the term is consistent
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_TERM_NOT_READY));
            }

            if (request.getTerm() < memberState.getLedgerEndTerm()) {
                // 如果发起投票节点的投票轮次小于ledgerEndTerm，则拒绝
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.getLedgerEndTerm()).voteResult(VoteResponse.RESULT.REJECT_TERM_SMALL_THAN_LEDGER));
            }

            if (!self && isTakingLeadership() && request.getLedgerEndTerm() == memberState.getLedgerEndTerm() && memberState.getLedgerEndIndex() >= request.getLedgerEndIndex()) {
                // 如果发起投票节点的ledgerEndTerm等于当前节点的ledgerEndTerm，并且ledgerEndIndex大于等于发起投票节点的ledgerEndIndex，因为这意味着当前节点的日志虽然和发起投票节点在同一轮次，但是当前节点的日志比投票发起者的更新，所以拒绝拉票。
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_TAKING_LEADERSHIP));
            }

            // 投票给请求节点
            memberState.setCurrVoteFor(request.getLeaderId());
            return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.ACCEPT));
        }
    }

    /**
     * 向集群内从节点发送心跳包
     * @param term
     * @param leaderId
     * @throws Exception
     */
    private void sendHeartbeats(long term, String leaderId) throws Exception {
        // 集群内节点个数
        final AtomicInteger allNum = new AtomicInteger(1);
        // 收到成功响应的节点个数
        final AtomicInteger succNum = new AtomicInteger(1);
        // 收到对端没有准备好反馈的节点数量
        final AtomicInteger notReadyNum = new AtomicInteger(0);
        // 当前集群中各个节点维护的最大的投票轮次
        final AtomicLong maxTerm = new AtomicLong(-1);
        // 是否存在leader节点不一致的情况
        final AtomicBoolean inconsistLeader = new AtomicBoolean(false);
        // 用于等待异步请求结果
        final CountDownLatch beatLatch = new CountDownLatch(1);
        // 本次心跳包开始发送的时间戳
        long startHeartbeatTimeMs = System.currentTimeMillis();
        for (String id : memberState.getPeerMap().keySet()) {
            if (memberState.getSelfId().equals(id)) {
                continue;
            }
            HeartBeatRequest heartBeatRequest = new HeartBeatRequest();
            heartBeatRequest.setGroup(memberState.getGroup());
            heartBeatRequest.setLocalId(memberState.getSelfId());
            heartBeatRequest.setRemoteId(id);
            heartBeatRequest.setLeaderId(leaderId);
            heartBeatRequest.setTerm(term);
            CompletableFuture<HeartBeatResponse> future = dLedgerRpcService.heartBeat(heartBeatRequest);
            future.whenComplete((HeartBeatResponse x, Throwable ex) -> {
                try {
                    if (ex != null) {
                        memberState.getPeersLiveTable().put(id, Boolean.FALSE);
                        throw ex;
                    }
                    // 当收到一个节点的响应结果后触发回调函数，统计响应结果
                    switch (DLedgerResponseCode.valueOf(x.getCode())) {
                        case SUCCESS:
                            succNum.incrementAndGet();
                            break;
                        case EXPIRED_TERM:
                            // 节点的投票轮次，小于从节点的投票轮次
                            maxTerm.set(x.getTerm());
                            break;
                        case INCONSISTENT_LEADER:
                            // 从节点已经有了新的主节点
                            inconsistLeader.compareAndSet(false, true);
                            break;
                        case TERM_NOT_READY:
                            // 从节点未准备好
                            notReadyNum.incrementAndGet();
                            break;
                        default:
                            break;
                    }

                    // 根据错误码，判断节点是否存活
                    if (x.getCode() == DLedgerResponseCode.NETWORK_ERROR.getCode())
                        memberState.getPeersLiveTable().put(id, Boolean.FALSE);
                    else
                        memberState.getPeersLiveTable().put(id, Boolean.TRUE);

                    // 如果收到SUCCESS的从节点数量超过集群节点的半数，唤醒主线程，
                    if (memberState.isQuorum(succNum.get())
                            || memberState.isQuorum(succNum.get() + notReadyNum.get())) {
                        beatLatch.countDown();
                    }
                } catch (Throwable t) {
                    LOGGER.error("heartbeat response failed", t);
                } finally {
                    allNum.incrementAndGet();
                    if (allNum.get() == memberState.peerSize()) {
                        // 如果收到所有从节点响应，唤醒主线程，
                        beatLatch.countDown();
                    }
                }
            });
        }
        long voteResultWaitTime = 10;
        // 如果收到SUCCESS的从节点数量超过集群节点的半数，或者收到集群内所有节点的响应结果后调用CountDownLatch的countDown()方法从而唤醒了主线程，则继续执行后续流程
        beatLatch.await(heartBeatTimeIntervalMs - voteResultWaitTime, TimeUnit.MILLISECONDS);
        Thread.sleep(voteResultWaitTime);

        //abnormal case, deal with it immediately
        if (maxTerm.get() > term) {
            // 如果从节点的选举任期大于当前节点，则立即将当前节点的状态更改为Candidate
            LOGGER.warn("[{}] currentTerm{} is not the biggest={}, deal with it", memberState.getSelfId(), term, maxTerm.get());
            changeRoleToCandidate(maxTerm.get());
            return;
        }

        if (memberState.isQuorum(succNum.get())) {
            // 如果当前Leader节点收到超过集群半数节点的认可(SUCCESS)，表示集群状态正常，则正常按照心跳包间隔发送心跳包。
            lastSuccHeartBeatTime = System.currentTimeMillis();
        } else {
            LOGGER.info("[{}] Parse heartbeat responses in cost={} term={} allNum={} succNum={} notReadyNum={} inconsistLeader={} maxTerm={} peerSize={} lastSuccHeartBeatTime={}",
                    memberState.getSelfId(), DLedgerUtils.elapsed(startHeartbeatTimeMs), term, allNum.get(), succNum.get(), notReadyNum.get(), inconsistLeader.get(), maxTerm.get(), memberState.peerSize(), new Timestamp(lastSuccHeartBeatTime));
            if (memberState.isQuorum(succNum.get() + notReadyNum.get())) {
                // 如果当前Leader节点收到SUCCESS的响应数加上未准备投票的节点数超过集群节点的半数，则立即发送心跳包。
                lastSendHeartBeatTime = -1;
            } else if (inconsistLeader.get()) {
                // 如果leader变成了其他节点，则将当前节点状态更改为Candidate。
                changeRoleToCandidate(term);
            } else if (DLedgerUtils.elapsed(lastSuccHeartBeatTime) > (long) maxHeartBeatLeak * heartBeatTimeIntervalMs) {
                // 最近成功发送心跳的时间戳超过最大允许的间隔时间，则将当前节点状态更改为Candidate。
                changeRoleToCandidate(term);
            }
        }
    }

    private void maintainAsLeader() throws Exception {
        if (DLedgerUtils.elapsed(lastSendHeartBeatTime) > heartBeatTimeIntervalMs) {
            // 如果当前时间与上一次发送心跳包的间隔时间大于一个心跳包周期（默认为2s），则进入心跳包发送处理逻辑，否则忽略。
            long term;
            String leaderId;
            synchronized (memberState) {
                if (!memberState.isLeader()) {
                    // 如果当前状态机的状态已经不是Leader，则忽略。
                    //stop sending
                    return;
                }
                term = memberState.currTerm();
                leaderId = memberState.getLeaderId();
                // 记录本次发送心跳包的时间戳。
                lastSendHeartBeatTime = System.currentTimeMillis();
            }
            // 调用sendHeartbeats()方法向集群内的从节点发送心跳包
            sendHeartbeats(term, leaderId);
        }
    }

    private void maintainAsFollower() {
        // 如果节点在maxHeartBeatLeak个心跳包（默认为3个）周期内未收
        // 到心跳包，则将状态变更为Candidate。从这里也不得不佩服RocketMQ
        // 在性能方面如此追求极致，即在不加锁的情况下判断是否超过了2个心
        // 跳包周期，减少加锁次数，提高性能。
        if (DLedgerUtils.elapsed(lastLeaderHeartBeatTime) > 2L * heartBeatTimeIntervalMs) {
            synchronized (memberState) {
                if (memberState.isFollower() && DLedgerUtils.elapsed(lastLeaderHeartBeatTime) > (long) maxHeartBeatLeak * heartBeatTimeIntervalMs) {
                    LOGGER.info("[{}][HeartBeatTimeOut] lastLeaderHeartBeatTime: {} heartBeatTimeIntervalMs: {} lastLeader={}", memberState.getSelfId(), new Timestamp(lastLeaderHeartBeatTime), heartBeatTimeIntervalMs, memberState.getLeaderId());
                    // 将节点状态更改为 Candidate
                    changeRoleToCandidate(memberState.currTerm());
                }
            }
        }
    }

    /**
     * 异步向集群其他节点发起投票请求求，并等待各个节点的响应结果
     * @param term
     * @param ledgerEndTerm
     * @param ledgerEndIndex
     * @return
     * @throws Exception
     */
    private List<CompletableFuture<VoteResponse>> voteForQuorumResponses(long term, long ledgerEndTerm,
                                                                         long ledgerEndIndex) throws Exception {
        List<CompletableFuture<VoteResponse>> responses = new ArrayList<>();
        for (String id : memberState.getPeerMap().keySet()) {
            VoteRequest voteRequest = new VoteRequest();
            voteRequest.setGroup(memberState.getGroup());
            voteRequest.setLedgerEndIndex(ledgerEndIndex);
            voteRequest.setLedgerEndTerm(ledgerEndTerm);
            voteRequest.setLeaderId(memberState.getSelfId());
            voteRequest.setTerm(term);
            voteRequest.setRemoteId(id);
            voteRequest.setLocalId(memberState.getSelfId());
            CompletableFuture<VoteResponse> voteResponse;
            if (memberState.getSelfId().equals(id)) {
                // 如果投票人是自己，则直接调用handleVote()方法处理投票请求，并返回处理结果。
                voteResponse = handleVote(voteRequest, true);
            } else {
                //async
                // 如果投票人不是自己，则调用dLedgerRpcService.vote()方法发起投票请求，并返回处理结果。
                voteResponse = dLedgerRpcService.vote(voteRequest);
            }
            responses.add(voteResponse);

        }
        return responses;
    }

    private boolean isTakingLeadership() {
        if (dLedgerConfig.getPreferredLeaderIds() != null && memberState.getTermToTakeLeadership() == memberState.currTerm()) {
            List<String> preferredLeaderIds = Arrays.asList(dLedgerConfig.getPreferredLeaderIds().split(";"));
            return preferredLeaderIds.contains(memberState.getSelfId());
        }
        return false;
    }

    private long getNextTimeToRequestVote() {
        if (isTakingLeadership()) {
            return System.currentTimeMillis() + dLedgerConfig.getMinTakeLeadershipVoteIntervalMs() +
                    random.nextInt(dLedgerConfig.getMaxTakeLeadershipVoteIntervalMs() - dLedgerConfig.getMinTakeLeadershipVoteIntervalMs());
        }
        return System.currentTimeMillis() + minVoteIntervalMs + random.nextInt(maxVoteIntervalMs - minVoteIntervalMs);
    }

    private void maintainAsCandidate() throws Exception {
        //for candidate
        // 下一次可发起投票的时间，如果当前时间小于该值，说明计时器未过期，此时无须发起投票
        if (System.currentTimeMillis() < nextTimeToRequestVote && !needIncreaseTermImmediately) {
            return;
        }
        // 投票轮次
        long term;
        // Leader节点当前的投票轮次。
        long ledgerEndTerm;
        // 当前节点日志的最大序列号，即下一条日志的开始index
        long ledgerEndIndex;
        if (!memberState.isCandidate()) {
            return;
        }
        synchronized (memberState) {
            // 双重校验锁，对状态机加锁后再次校验状态机状态是否为Candidate，既保证了并发性能，又能解决并发安全问题
            if (!memberState.isCandidate()) {
                return;
            }
            if (lastParseResult == VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT || needIncreaseTermImmediately) {
                long prevTerm = memberState.currTerm();
                term = memberState.nextTerm();
                LOGGER.info("{}_[INCREASE_TERM] from {} to {}", memberState.getSelfId(), prevTerm, term);
                lastParseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            } else {
                // 如果上一次的投票结果不是WAIT_TO_VOTE_NEXT，则投票轮次依然为状态机内部维护的投票轮次。
                term = memberState.currTerm();
            }
            ledgerEndIndex = memberState.getLedgerEndIndex();
            ledgerEndTerm = memberState.getLedgerEndTerm();
        }
        if (needIncreaseTermImmediately) {
            // 如果needIncreaseTermImmediately为true，则重置该标
            //记位为false，并重新设置下一次投票超时时间，其实现逻辑为当前时
            //间戳+上次投票的开销+最小投票间隔之间的随机值，这里是Raft协议
            //的一个关键点，即每个节点的投票超时时间引入了随机值
            nextTimeToRequestVote = getNextTimeToRequestVote();
            needIncreaseTermImmediately = false;
            return;
        }

        long startVoteTimeMs = System.currentTimeMillis();
        // 向集群其他节点发起投票请求求，并等待各个节点的响应结果。
        final List<CompletableFuture<VoteResponse>> quorumVoteResponses = voteForQuorumResponses(term, ledgerEndTerm, ledgerEndIndex);
        // 已知的最大投票轮次
        final AtomicLong knownMaxTermInGroup = new AtomicLong(term);
        // 所有投票数
        final AtomicInteger allNum = new AtomicInteger(0);
        // 有效投票数
        final AtomicInteger validNum = new AtomicInteger(0);
        // 赞成票数量
        final AtomicInteger acceptedNum = new AtomicInteger(0);
        // 未准备投票的节点数量，如果对端节点的投票轮次小于发起投票的轮次，则认为对端未准备好，对端节点使用本轮次进入Candidate状态。
        final AtomicInteger notReadyTermNum = new AtomicInteger(0);
        // 发起投票的节点的ledgerEndTerm小于对端节点的个数
        final AtomicInteger biggerLedgerNum = new AtomicInteger(0);
        // 是否已经存在Leader
        final AtomicBoolean alreadyHasLeader = new AtomicBoolean(false);

        // 在上面异步向集群内的各个节点发送投票请求，接下来需要同步等待所有的响应结果。这里RocketMQ向我们展示了一种非
        // 常优雅的编程技巧，在收到对端的响应结果后触发CountDownLatch与Future的whenComplete方法。在业务处理过程中，如果条件满足则调
        // 用CountDownLatch的countDown方法，唤醒await()方法，使之接受全部响应结果后执行后续逻辑。
        CountDownLatch voteLatch = new CountDownLatch(1);
        for (CompletableFuture<VoteResponse> future : quorumVoteResponses) {
            future.whenComplete((VoteResponse x, Throwable ex) -> {
                try {
                    if (ex != null) {
                        throw ex;
                    }
                    LOGGER.info("[{}][GetVoteResponse] {}", memberState.getSelfId(), JSON.toJSONString(x));
                    if (x.getVoteResult() != VoteResponse.RESULT.UNKNOWN) {
                        validNum.incrementAndGet();
                    }
                    synchronized (knownMaxTermInGroup) {
                        switch (x.getVoteResult()) {
                            case ACCEPT:
                                // 赞成票（acceptedNum）加1，只有得到的赞成票超过集群节点数量的一半才能成为Leader。
                                acceptedNum.incrementAndGet();
                                break;
                            case REJECT_ALREADY_HAS_LEADER:
                                // 拒绝票，原因是集群中已经存在Leaer节点了。alreadyHasLeader设置为true，无须再判断其他投票结果了，结束本轮投票。
                                alreadyHasLeader.compareAndSet(false, true);
                                break;
                            case REJECT_TERM_SMALL_THAN_LEDGER:
                                // 拒绝票，原因是自己维护的term小于远端维护的ledgerEndTerm。如果对端的team大于自己的
                                // team，需要记录对端最大的投票轮次，以便更新自己的投票轮次
                            case REJECT_EXPIRED_VOTE_TERM:
                                // 拒绝票，原因是自己维护的投票轮次小于远端维护的投票轮次，并且更新自己维护的投票轮次
                                if (x.getTerm() > knownMaxTermInGroup.get()) {
                                    knownMaxTermInGroup.set(x.getTerm());
                                }
                                break;
                            case REJECT_EXPIRED_LEDGER_TERM:
                                // 拒绝票，原因是自己维护的ledgerTerm小于对端维护的ledgerTerm，此种情况下需要增加计数器
                                //biggerLedgerNum的值。
                            case REJECT_SMALL_LEDGER_END_INDEX:
                                // 拒绝票，原因是对端的ledgerTeam与自己维护的ledgerTeam相等，但自己维护的
                                //dedgerEndIndex小于对端维护的值，这种情况下需要增加biggerLedgerNum计数器的值。
                                biggerLedgerNum.incrementAndGet();
                                break;
                            case REJECT_TERM_NOT_READY:
                                // 拒绝票，原因是对端的投票轮次小于自己的投票轮次，即对端还未准备好投票。此时对端节点使用自己
                                // 的投票轮次进入Candidate状态。
                                notReadyTermNum.incrementAndGet();
                                break;
                            case REJECT_ALREADY_VOTED:// 拒绝票，原因是已经投给了其他节点
                            case REJECT_TAKING_LEADERSHIP://拒绝票，原因是对端的投票轮次和自己相等，但是对端节点的ledgerEndIndex比自己的ledgerEndIndex大，这意味着对端节点的日志状态更新。
                            default:
                                break;

                        }
                    }
                    if (alreadyHasLeader.get()
                            || memberState.isQuorum(acceptedNum.get())
                            || memberState.isQuorum(acceptedNum.get() + notReadyTermNum.get())) {
                        voteLatch.countDown();
                    }
                } catch (Throwable t) {
                    LOGGER.error("vote response failed", t);
                } finally {
                    allNum.incrementAndGet();
                    if (allNum.get() == memberState.peerSize()) {
                        voteLatch.countDown();
                    }
                }
            });

        }

        try {
            // 因为投票结果的统计是异步的，这里等待投票结果统计完成。
            voteLatch.await(2000 + random.nextInt(maxVoteIntervalMs), TimeUnit.MILLISECONDS);
        } catch (Throwable ignore) {

        }

        // 投票耗时
        lastVoteCost = DLedgerUtils.elapsed(startVoteTimeMs);
        VoteResponse.ParseResult parseResult;
        if (knownMaxTermInGroup.get() > term) {
            // 如果对端的投票轮次大于当前节点维护的投票轮次，则先重置
            // 投票计时器，然后在定时器到期后使用对端的投票轮次重新进入
            //Candidate状态。
            parseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
            nextTimeToRequestVote = getNextTimeToRequestVote();
            changeRoleToCandidate(knownMaxTermInGroup.get());
        } else if (alreadyHasLeader.get()) {
            // 如果集群内已经存在Leader节点，当前节点将继续保持
            //Candidate状态，重置计时器，但这个计时器还需要增加
            //heartBeatTimeIntervalMs*maxHeartBeatLeak，其中
            //heartBeatTimeIntervalMs为一次心跳间隔时间，maxHeartBeatLeak为
            //允许丢失的最大心跳包。增加这个时间是因为集群内既然已经存在
            //Leader节点了，就会在一个心跳周期内发送心跳包，从节点在收到心
            //跳包后会重置定时器，即阻止Follower节点进入Candidate状态。这样
            //做的目的是在指定时间内收到Leader节点的心跳包，从而驱动当前节
            //点的状态由Candidate向Follower转换
            parseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            nextTimeToRequestVote = getNextTimeToRequestVote() + (long) heartBeatTimeIntervalMs * maxHeartBeatLeak;
        } else if (!memberState.isQuorum(validNum.get())) {
            // 如果收到的有效票数未超过半数，则重置计时器并等待重新投
            //票，注意当前状态为WAIT_TO_REVOTE，该状态下的特征是下次投票时
            //不增加投票轮次。
            parseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            nextTimeToRequestVote = getNextTimeToRequestVote();
        } else if (!memberState.isQuorum(validNum.get() - biggerLedgerNum.get())) {
            parseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            nextTimeToRequestVote = getNextTimeToRequestVote() + maxVoteIntervalMs;
        } else if (memberState.isQuorum(acceptedNum.get())) {
            // 如果得到的赞同票超过半数，则成为Leader节点，
            parseResult = VoteResponse.ParseResult.PASSED;
        } else if (memberState.isQuorum(acceptedNum.get() + notReadyTermNum.get())) {
            // 如果得到的赞成票加上未准备投票的节点数超过半数，则立即
            //发起投票，故其结果为REVOTE_IMMEDIATELY。
            parseResult = VoteResponse.ParseResult.REVOTE_IMMEDIATELY;
        } else {
            parseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
            nextTimeToRequestVote = getNextTimeToRequestVote();
        }
        lastParseResult = parseResult;
        LOGGER.info("[{}] [PARSE_VOTE_RESULT] cost={} term={} memberNum={} allNum={} acceptedNum={} notReadyTermNum={} biggerLedgerNum={} alreadyHasLeader={} maxTerm={} result={}",
                memberState.getSelfId(), lastVoteCost, term, memberState.peerSize(), allNum, acceptedNum, notReadyTermNum, biggerLedgerNum, alreadyHasLeader, knownMaxTermInGroup.get(), parseResult);

        if (parseResult == VoteResponse.ParseResult.PASSED) {
            LOGGER.info("[{}] [VOTE_RESULT] has been elected to be the leader in term {}", memberState.getSelfId(), term);
            // 调用changeRoleToLeader方法驱动状态机向Leader状态转换。
            changeRoleToLeader(term);
        }
    }

    /**
     * The core method of maintainer. Run the specified logic according to the current role: candidate => propose a
     * vote. leader => send heartbeats to followers, and step down to candidate when quorum followers do not respond.
     * follower => accept heartbeats, and change to candidate when no heartbeat from leader.
     *
     * @throws Exception
     */
    private void maintainState() throws Exception {
        // 如果是leader状态
        if (memberState.isLeader()) {
            // leader状态、主节点，该状态下需要定时向从节点发送心跳包，用于传播数据、确保其领导地位
            maintainAsLeader();
        } else if (memberState.isFollower()) {
            // follower状态，该状态下会开启定时器，尝试进入Candidate状态，以便发起投票选举，一旦收到主节点的心跳包，则重置定时器
            maintainAsFollower();
        } else {
            // Candidate（候选者）状态，该状态下的节点会发起投票，尝试选择自己为主节点，选举成功后，不会存在该状态下的节点
            maintainAsCandidate();
        }
    }

    private void handleRoleChange(long term, MemberState.Role role) {
        try {
            takeLeadershipTask.check(term, role);
        } catch (Throwable t) {
            LOGGER.error("takeLeadershipTask.check failed. ter={}, role={}", term, role, t);
        }

        for (RoleChangeHandler roleChangeHandler : roleChangeHandlers) {
            try {
                roleChangeHandler.handle(term, role);
            } catch (Throwable t) {
                LOGGER.warn("Handle role change failed term={} role={} handler={}", term, role, roleChangeHandler.getClass(), t);
            }
        }
    }

    public void addRoleChangeHandler(RoleChangeHandler roleChangeHandler) {
        if (!roleChangeHandlers.contains(roleChangeHandler)) {
            roleChangeHandlers.add(roleChangeHandler);
        }
    }

    public CompletableFuture<LeadershipTransferResponse> handleLeadershipTransfer(
            LeadershipTransferRequest request) throws Exception {
        LOGGER.info("handleLeadershipTransfer: {}", request);
        synchronized (memberState) {
            if (memberState.currTerm() != request.getTerm()) {
                LOGGER.warn("[BUG] [HandleLeaderTransfer] currTerm={} != request.term={}", memberState.currTerm(), request.getTerm());
                return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.INCONSISTENT_TERM.getCode()));
            }

            if (!memberState.isLeader()) {
                LOGGER.warn("[BUG] [HandleLeaderTransfer] selfId={} is not leader", request.getLeaderId());
                return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.NOT_LEADER.getCode()));
            }

            if (memberState.getTransferee() != null) {
                LOGGER.warn("[BUG] [HandleLeaderTransfer] transferee={} is already set", memberState.getTransferee());
                return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.LEADER_TRANSFERRING.getCode()));
            }

            memberState.setTransferee(request.getTransfereeId());
        }
        LeadershipTransferRequest takeLeadershipRequest = new LeadershipTransferRequest();
        takeLeadershipRequest.setGroup(memberState.getGroup());
        takeLeadershipRequest.setLeaderId(memberState.getLeaderId());
        takeLeadershipRequest.setLocalId(memberState.getSelfId());
        takeLeadershipRequest.setRemoteId(request.getTransfereeId());
        takeLeadershipRequest.setTerm(request.getTerm());
        takeLeadershipRequest.setTakeLeadershipLedgerIndex(memberState.getLedgerEndIndex());
        takeLeadershipRequest.setTransferId(memberState.getSelfId());
        takeLeadershipRequest.setTransfereeId(request.getTransfereeId());
        if (memberState.currTerm() != request.getTerm()) {
            LOGGER.warn("[HandleLeaderTransfer] term changed, cur={} , request={}", memberState.currTerm(), request.getTerm());
            return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.EXPIRED_TERM.getCode()));
        }

        return dLedgerRpcService.leadershipTransfer(takeLeadershipRequest).thenApply(response -> {
            synchronized (memberState) {
                if (response.getCode() != DLedgerResponseCode.SUCCESS.getCode() ||
                        (memberState.currTerm() == request.getTerm() && memberState.getTransferee() != null)) {
                    LOGGER.warn("leadershipTransfer failed, set transferee to null");
                    memberState.setTransferee(null);
                }
            }
            return response;
        });
    }

    public CompletableFuture<LeadershipTransferResponse> handleTakeLeadership(
            LeadershipTransferRequest request) {
        LOGGER.debug("handleTakeLeadership.request={}", request);
        synchronized (memberState) {
            if (memberState.currTerm() != request.getTerm()) {
                LOGGER.warn("[BUG] [handleTakeLeadership] currTerm={} != request.term={}", memberState.currTerm(), request.getTerm());
                return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.INCONSISTENT_TERM.getCode()));
            }

            long targetTerm = request.getTerm() + 1;
            memberState.setTermToTakeLeadership(targetTerm);
            CompletableFuture<LeadershipTransferResponse> response = new CompletableFuture<>();
            takeLeadershipTask.update(request, response);
            changeRoleToCandidate(targetTerm);
            needIncreaseTermImmediately = true;
            return response;
        }
    }

    private class TakeLeadershipTask {
        private LeadershipTransferRequest request;
        private CompletableFuture<LeadershipTransferResponse> responseFuture;

        public synchronized void update(LeadershipTransferRequest request,
                                        CompletableFuture<LeadershipTransferResponse> responseFuture) {
            this.request = request;
            this.responseFuture = responseFuture;
        }

        public synchronized void check(long term, MemberState.Role role) {
            LOGGER.trace("TakeLeadershipTask called, term={}, role={}", term, role);
            if (memberState.getTermToTakeLeadership() == -1 || responseFuture == null) {
                return;
            }
            LeadershipTransferResponse response = null;
            if (term > memberState.getTermToTakeLeadership()) {
                response = new LeadershipTransferResponse().term(term).code(DLedgerResponseCode.EXPIRED_TERM.getCode());
            } else if (term == memberState.getTermToTakeLeadership()) {
                switch (role) {
                    case LEADER:
                        response = new LeadershipTransferResponse().term(term).code(DLedgerResponseCode.SUCCESS.getCode());
                        break;
                    case FOLLOWER:
                        response = new LeadershipTransferResponse().term(term).code(DLedgerResponseCode.TAKE_LEADERSHIP_FAILED.getCode());
                        break;
                    default:
                        return;
                }
            } else {
                switch (role) {
                    /*
                     * The node may receive heartbeat before term increase as a candidate,
                     * then it will be follower and term < TermToTakeLeadership
                     */
                    case FOLLOWER:
                        response = new LeadershipTransferResponse().term(term).code(DLedgerResponseCode.TAKE_LEADERSHIP_FAILED.getCode());
                        break;
                    default:
                        response = new LeadershipTransferResponse().term(term).code(DLedgerResponseCode.INTERNAL_ERROR.getCode());
                }
            }

            responseFuture.complete(response);
            LOGGER.info("TakeLeadershipTask finished. request={}, response={}, term={}, role={}", request, response, term, role);
            memberState.setTermToTakeLeadership(-1);
            responseFuture = null;
            request = null;
        }
    }

    public interface RoleChangeHandler {
        void handle(long term, MemberState.Role role);

        void startup();

        void shutdown();
    }

    public class StateMaintainer extends ShutdownAbleThread {

        public StateMaintainer(String name, Logger logger) {
            super(name, logger);
        }

        @Override
        public void doWork() {
            try {
                // 如果当前节点参与Leader选举，则调用maintainState()方法驱动状态机，并且每一次驱动状态机后休息10ms
                if (DLedgerLeaderElector.this.dLedgerConfig.isEnableLeaderElector()) {
                    DLedgerLeaderElector.this.refreshIntervals(dLedgerConfig);
                    DLedgerLeaderElector.this.maintainState();
                }
                sleep(10);
            } catch (Throwable t) {
                DLedgerLeaderElector.LOGGER.error("Error in heartbeat", t);
            }
        }

    }
}
