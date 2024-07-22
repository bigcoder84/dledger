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
import io.openmessaging.storage.dledger.common.Closure;
import io.openmessaging.storage.dledger.common.ShutdownAbleThread;
import io.openmessaging.storage.dledger.common.Status;
import io.openmessaging.storage.dledger.common.TimeoutFuture;
import io.openmessaging.storage.dledger.common.WriteClosure;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.exception.DLedgerException;
import io.openmessaging.storage.dledger.metrics.DLedgerMetricsManager;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.InstallSnapshotRequest;
import io.openmessaging.storage.dledger.protocol.InstallSnapshotResponse;
import io.openmessaging.storage.dledger.protocol.PushEntryRequest;
import io.openmessaging.storage.dledger.protocol.PushEntryResponse;
import io.openmessaging.storage.dledger.snapshot.DownloadSnapshot;
import io.openmessaging.storage.dledger.snapshot.SnapshotManager;
import io.openmessaging.storage.dledger.snapshot.SnapshotMeta;
import io.openmessaging.storage.dledger.snapshot.SnapshotReader;
import io.openmessaging.storage.dledger.statemachine.ApplyEntry;
import io.openmessaging.storage.dledger.statemachine.StateMachineCaller;
import io.openmessaging.storage.dledger.store.DLedgerStore;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import io.openmessaging.storage.dledger.utils.Pair;
import io.openmessaging.storage.dledger.utils.PreConditions;
import io.openmessaging.storage.dledger.utils.Quota;

import io.opentelemetry.api.common.Attributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.openmessaging.storage.dledger.metrics.DLedgerMetricsConstant.LABEL_REMOTE_ID;

/**
 * 日志复制由该类实现
 */
public class DLedgerEntryPusher {

    private static final Logger LOGGER = LoggerFactory.getLogger(DLedgerEntryPusher.class);

    /**
     * 多副本相关配置。
     */
    private final DLedgerConfig dLedgerConfig;
    /**
     * 存储实现类。
     */
    private final DLedgerStore dLedgerStore;
    /**
     * 节点状态机。
     */
    private final MemberState memberState;
    /**
     * RPC 服务实现类，用于集群内的其他节点进行网络通讯。
     */
    private final DLedgerRpcService dLedgerRpcService;

    /**
     * 每个节点基于投票轮次的当前水位线标记。
     * 用于记录从节点已复制的日志序号
     */
    private final Map<Long/*term*/, ConcurrentMap<String/*peer id*/, Long/*match index*/>> peerWaterMarksByTerm = new ConcurrentHashMap<>();

    /**
     * 正在处理的 apend 请求的回调函数。放在这里的index所指向的日志是待确认的日志，也就是说客户端目前正处在阻塞状态，等待从节点接收日志。
     *
     * 当日志写入Leader节点后，会异步将日志发送给Follower节点，当集群中大多数节点成功写入该日志后，会回调这里暂存的回调函数，从而返回客户端成功写入的状态。
     */
    private final Map<Long/*term*/, ConcurrentMap<Long/*index*/, Closure/*upper callback*/>> pendingClosure = new ConcurrentHashMap<>();

    /**
     * 从节点上开启的线程，用于接收主节点的 push 请求（append、commit）。
     */
    private final EntryHandler entryHandler;
    /**
     * 日志追加ACK投票仲裁线程，用于判断日志是否可提交，当前节点为主节点时激活
     */
    private final QuorumAckChecker quorumAckChecker;
    /**
     * 日志请求转发器，负责向从节点转发日志，主节点为每一个从节点构建一个EntryDispatcher，EntryDispatcher是一个线程
     */
    private final Map<String/*peer id*/, EntryDispatcher/*entry dispatcher for each peer*/> dispatcherMap = new HashMap<>();
    /**
     * 当前节点的ID
     */
    private final String selfId;
    /**
     * 通过任务队列修改状态机状态，保证所有修改状态机状态的任务按顺序执行
     */
    private StateMachineCaller fsmCaller;

    public DLedgerEntryPusher(DLedgerConfig dLedgerConfig, MemberState memberState, DLedgerStore dLedgerStore,
        DLedgerRpcService dLedgerRpcService) {
        this.dLedgerConfig = dLedgerConfig;
        this.selfId = this.dLedgerConfig.getSelfId();
        this.memberState = memberState;
        this.dLedgerStore = dLedgerStore;
        this.dLedgerRpcService = dLedgerRpcService;
        // 为每一个Follower节点创建一个EntryDispatcher线程，复制向Follower节点推送日志
        for (String peer : memberState.getPeerMap().keySet()) {
            if (!peer.equals(memberState.getSelfId())) {
                dispatcherMap.put(peer, new EntryDispatcher(peer, LOGGER));
            }
        }
        this.entryHandler = new EntryHandler(LOGGER);
        this.quorumAckChecker = new QuorumAckChecker(LOGGER);
    }

    /**
     * 启动日志复制逻辑，当节点为不同身份时，生效的线程并不一样（未生效的线程间隔1ms空转，等待节点身份变化）：
     * Leader：EntryDispatcher QuorumAckChecker
     * Follower：EntryHandler
     */
    public void startup() {
        // 启动 EntryHandler，负责接受Leader节点推送的日志，如果节点不是Follower节点现成也会启动，但是不会执行任何逻辑，直到身份变成Follower节点。
        entryHandler.start();
        // 启动 日志追加ACK投票仲裁线程，用于判断日志是否可提交，当前节点为Leader节点时激活
        quorumAckChecker.start();
        // 启动 日志分发线程，用于向Follower节点推送日志，当前节点为Leader节点时激活
        for (EntryDispatcher dispatcher : dispatcherMap.values()) {
            dispatcher.start();
        }
    }

    public void shutdown() {
        entryHandler.shutdown();
        quorumAckChecker.shutdown();
        for (EntryDispatcher dispatcher : dispatcherMap.values()) {
            dispatcher.shutdown();
        }
    }

    public void registerStateMachine(final StateMachineCaller fsmCaller) {
        this.fsmCaller = fsmCaller;
    }

    public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        return entryHandler.handlePush(request);
    }

    public CompletableFuture<InstallSnapshotResponse> handleInstallSnapshot(InstallSnapshotRequest request) {
        return entryHandler.handleInstallSnapshot(request);
    }

    private void checkTermForWaterMark(long term, String env) {
        if (!peerWaterMarksByTerm.containsKey(term)) {
            LOGGER.info("Initialize the watermark in {} for term={}", env, term);
            ConcurrentMap<String, Long> waterMarks = new ConcurrentHashMap<>();
            for (String peer : memberState.getPeerMap().keySet()) {
                waterMarks.put(peer, -1L);
            }
            peerWaterMarksByTerm.putIfAbsent(term, waterMarks);
        }
    }

    private void checkTermForPendingMap(long term, String env) {
        if (!pendingClosure.containsKey(term)) {
            LOGGER.info("Initialize the pending closure map in {} for term={}", env, term);
            pendingClosure.putIfAbsent(term, new ConcurrentHashMap<>());
        }

    }

    private void updatePeerWaterMark(long term, String peerId, long index) {
        synchronized (peerWaterMarksByTerm) {
            checkTermForWaterMark(term, "updatePeerWaterMark");
            if (peerWaterMarksByTerm.get(term).get(peerId) < index) {
                peerWaterMarksByTerm.get(term).put(peerId, index);
            }
        }
    }

    public long getPeerWaterMark(long term, String peerId) {
        synchronized (peerWaterMarksByTerm) {
            checkTermForWaterMark(term, "getPeerWaterMark");
            return peerWaterMarksByTerm.get(term).get(peerId);
        }
    }

    /**
     * 判断队列是否已满
     *
     * @param currTerm
     * @return
     */
    public boolean isPendingFull(long currTerm) {
        checkTermForPendingMap(currTerm, "isPendingFull");
        // 每一个投票轮次积压的日志数量默认不超过10000条，可通过配置改变该值
        return pendingClosure.get(currTerm).size() > dLedgerConfig.getMaxPendingRequestsNum();
    }

    public void appendClosure(Closure closure, long term, long index) {
        updatePeerWaterMark(term, memberState.getSelfId(), index);
        checkTermForPendingMap(term, "waitAck");
        Closure old = this.pendingClosure.get(term).put(index, closure);
        if (old != null) {
            LOGGER.warn("[MONITOR] get old wait at term = {}, index= {}", term, index);
        }
    }

    public void wakeUpDispatchers() {
        for (EntryDispatcher dispatcher : dispatcherMap.values()) {
            dispatcher.wakeup();
        }
    }

    /**
     * Complete the TimeoutFuture in pendingAppendResponsesByTerm (CurrentTerm, index).
     * Called by statemachineCaller when a committed entry (CurrentTerm, index) was applying to statemachine done.
     *
     * @param task committed entry
     * @return true if complete success
     */
    public boolean completeResponseFuture(final ApplyEntry task) {
        final long index = task.getEntry().getIndex();
        final long term = this.memberState.currTerm();
        ConcurrentMap<Long, Closure> closureMap = this.pendingClosure.get(term);
        if (closureMap != null) {
            Closure closure = closureMap.remove(index);
            if (closure != null) {
                if (closure instanceof WriteClosure) {
                    WriteClosure writeClosure = (WriteClosure) closure;
                    writeClosure.setResp(task.getResp());
                }
                closure.done(Status.ok());
                LOGGER.info("Complete closure, term = {}, index = {}", term, index);
                return true;
            }
        }
        return false;
    }

    /**
     * Check responseFutures timeout from {beginIndex} in currentTerm
     *
     * @param beginIndex the beginning index to check
     */
    public void checkResponseFuturesTimeout(final long beginIndex) {
        final long term = this.memberState.currTerm();
        long maxIndex = this.memberState.getCommittedIndex() + dLedgerConfig.getMaxPendingRequestsNum() + 1;
        if (maxIndex > this.memberState.getLedgerEndIndex()) {
            maxIndex = this.memberState.getLedgerEndIndex() + 1;
        }
        ConcurrentMap<Long, Closure> closureMap = this.pendingClosure.get(term);
        if (closureMap != null && closureMap.size() > 0) {
            for (long i = beginIndex; i < maxIndex; i++) {
                Closure closure = closureMap.get(i);
                if (closure == null) {
                    // index may be removed for complete, we should continue scan
                } else if (closure.isTimeOut()) {
                    closure.done(Status.error(DLedgerResponseCode.WAIT_QUORUM_ACK_TIMEOUT));
                    closureMap.remove(i);
                } else {
                    break;
                }
            }
        }
    }

    /**
     * Check responseFutures elapsed before {endIndex} in currentTerm
     */
    private void checkResponseFuturesElapsed(final long endIndex) {
        final long currTerm = this.memberState.currTerm();
        final Map<Long, Closure> closureMap = this.pendingClosure.get(currTerm);
        for (Map.Entry<Long, Closure> closureEntry : closureMap.entrySet()) {
            if (closureEntry.getKey() <= endIndex) {
                closureEntry.getValue().done(Status.ok());
                closureMap.remove(closureEntry.getKey());
            }
        }
    }

    /**
     * Leader节点对日志复制进行仲裁，如果成功存储该条目日志的节点超过半数节点，则向客户端返回写入成功。
     * This thread will check the quorum index and complete the pending requests.
     */
    private class QuorumAckChecker extends ShutdownAbleThread {

        private long lastPrintWatermarkTimeMs = System.currentTimeMillis();
        private long lastCheckLeakTimeMs = System.currentTimeMillis();
        private long lastCheckTimeoutTimeMs = System.currentTimeMillis();

        public QuorumAckChecker(Logger logger) {
            super("QuorumAckChecker-" + memberState.getSelfId(), logger);
        }

        @Override
        public void doWork() {
            try {
                if (DLedgerUtils.elapsed(lastPrintWatermarkTimeMs) > 3000) {
                    logger.info("[{}][{}] term={} ledgerBeforeBegin={} ledgerEnd={} committed={} watermarks={} appliedIndex={}",
                        memberState.getSelfId(), memberState.getRole(), memberState.currTerm(), dLedgerStore.getLedgerBeforeBeginIndex(), dLedgerStore.getLedgerEndIndex(), memberState.getCommittedIndex(), JSON.toJSONString(peerWaterMarksByTerm), memberState.getAppliedIndex());
                    lastPrintWatermarkTimeMs = System.currentTimeMillis();
                }
                
                long currTerm = memberState.currTerm();
                checkTermForPendingMap(currTerm, "QuorumAckChecker");
                checkTermForWaterMark(currTerm, "QuorumAckChecker");
                // clear pending closure in old term
                if (pendingClosure.size() > 1) {
                    for (Long term : pendingClosure.keySet()) {
                        if (term == currTerm) {
                            continue;
                        }
                        for (Map.Entry<Long, Closure> futureEntry : pendingClosure.get(term).entrySet()) {
                            logger.info("[TermChange] Will clear the pending closure index={} for term changed from {} to {}", futureEntry.getKey(), term, currTerm);
                            futureEntry.getValue().done(Status.error(DLedgerResponseCode.EXPIRED_TERM));
                        }
                        pendingClosure.remove(term);
                    }
                }
                // clear peer watermarks in old term
                if (peerWaterMarksByTerm.size() > 1) {
                    for (Long term : peerWaterMarksByTerm.keySet()) {
                        if (term == currTerm) {
                            continue;
                        }
                        logger.info("[TermChange] Will clear the watermarks for term changed from {} to {}", term, currTerm);
                        peerWaterMarksByTerm.remove(term);
                    }
                }

                // clear the pending closure which index <= applyIndex
                if (DLedgerUtils.elapsed(lastCheckLeakTimeMs) > 1000) {
                    checkResponseFuturesElapsed(DLedgerEntryPusher.this.memberState.getAppliedIndex());
                    lastCheckLeakTimeMs = System.currentTimeMillis();
                }
                if (DLedgerUtils.elapsed(lastCheckTimeoutTimeMs) > 1000) {
                    // clear the timeout pending closure should check all since it can timeout for different index
                    checkResponseFuturesTimeout(DLedgerEntryPusher.this.memberState.getAppliedIndex() + 1);
                    lastCheckTimeoutTimeMs = System.currentTimeMillis();
                }
                if (!memberState.isLeader()) {
                    waitForRunning(1);
                    return;
                }

                // 更新当前节点的水位线
                updatePeerWaterMark(currTerm, memberState.getSelfId(), dLedgerStore.getLedgerEndIndex());

                // calculate the median of watermarks(which we can ensure that more than half of the nodes have been pushed the corresponding entry)
                // we can also call it quorumIndex
                // 计算所有节点水位线的中位数，那么理论上比这个中位数小的index来说都已经存储在集群中大多数节点上了。
                Map<String, Long> peerWaterMarks = peerWaterMarksByTerm.get(currTerm);
                List<Long> sortedWaterMarks = peerWaterMarks.values()
                    .stream()
                    .sorted(Comparator.reverseOrder())
                    .collect(Collectors.toList());
                long quorumIndex = sortedWaterMarks.get(sortedWaterMarks.size() / 2);

                // advance the commit index
                // we can only commit the index whose term is equals to current term (refer to raft paper 5.4.2)
                if (DLedgerEntryPusher.this.memberState.leaderUpdateCommittedIndex(currTerm, quorumIndex)) {
                    DLedgerEntryPusher.this.fsmCaller.onCommitted(quorumIndex);
                } else {
                    // If the commit index is not advanced, we should wait for the next round
                    waitForRunning(1);
                }
            } catch (Throwable t) {
                DLedgerEntryPusher.LOGGER.error("Error in {}", getName(), t);
                DLedgerUtils.sleep(100);
            }
        }
    }

    /**
     * 这个线程将由领导激活。这个线程将把日志条目推送给follower(由peerId标识)，并将完成推送的索引更新到索引映射。应该为每个follower生成一个线程。
     * 推送有4个类型:
     *  APPEND:添加条目
     *  COMPARE:如果领导发生变化,新领导人应比较其条目
     *  TRUNCATE:如果leader完成了一个索引的比较，leader将发送一个请求来截断follower的ledger
     *  COMMIT:通常,领导将附加已提交索引到APPEND请求中,但是如果追加请求很少和分散,则领导将发送纯请求来通知跟随者已提交索引
     *
     * This thread will be activated by the leader.
     * This thread will push the entry to follower(identified by peerId) and update the completed pushed index to index map.
     * Should generate a single thread for each peer.
     * The push has 4 types:
     * APPEND : append the entries to the follower
     * COMPARE : if the leader changes, the new leader should compare its entries to follower's
     * TRUNCATE : if the leader finished comparing by an index, the leader will send a request to truncate the follower's ledger
     * COMMIT: usually, the leader will attach the committed index with the APPEND request, but if the append requests are few and scattered,
     * the leader will send a pure request to inform the follower of committed index.
     * <p>
     * The common transferring between these types are as following:
     * <p>
     * COMPARE ---- TRUNCATE ---- APPEND ---- COMMIT
     * ^                             |
     * |---<-----<------<-------<----|
     */
    private class EntryDispatcher extends ShutdownAbleThread {

        /**
         * 向从节点发送命令的类型
         */
        private final AtomicReference<EntryDispatcherState> type = new AtomicReference<>(EntryDispatcherState.COMPARE);
        /**
         * 上一次发送commit请求的时间戳。
         */
        private long lastPushCommitTimeMs = -1;
        /**
         * 目标节点ID
         */
        private final String peerId;

        /**
         * 已写入的日志序号
         */
        private long writeIndex = DLedgerEntryPusher.this.dLedgerStore.getLedgerEndIndex() + 1;

        /**
         * the index of the last entry to be pushed to this peer(initialized to -1)
         */
        private long matchIndex = -1;

        private final int maxPendingSize = 1000;
        /**
         * Leader节点当前的投票轮次
         */
        private long term = -1;
        /**
         * Leader节点ID
         */
        private String leaderId = null;
        /**
         * 上次检测泄露的时间，所谓泄露，指的是挂起的日志请求数量超过了maxPendingSize。
         */
        private long lastCheckLeakTimeMs = System.currentTimeMillis();

        /**
         * 记录日志的挂起时间，key表示日志的序列（entryIndex），value表示挂起时间戳。
         */
        private final ConcurrentMap<Long/*index*/, Pair<Long/*send timestamp*/, Integer/*entries count in req*/>> pendingMap = new ConcurrentHashMap<>();
        /**
         * 需要批量push的日志数据
         */
        private final PushEntryRequest batchAppendEntryRequest = new PushEntryRequest();

        private long lastAppendEntryRequestSendTimeMs = -1;

        /**
         * 配额。
         */
        private final Quota quota = new Quota(dLedgerConfig.getPeerPushQuota());

        public EntryDispatcher(String peerId, Logger logger) {
            super("EntryDispatcher-" + memberState.getSelfId() + "-" + peerId, logger);
            this.peerId = peerId;
        }

        @Override
        public synchronized void start() {
            super.start();
            // initialize write index
            writeIndex = DLedgerEntryPusher.this.dLedgerStore.getLedgerEndIndex() + 1;
        }

        private boolean checkNotLeaderAndFreshState() {
            if (!memberState.isLeader()) {
                // 如果当前节点的状态不是Leader则直接返回。
                return true;
            }
            if (term != memberState.currTerm() || leaderId == null || !leaderId.equals(memberState.getLeaderId())) {
                // 如果日志转发器（EntryDispatcher）的投票轮次为空或与状态机的投票轮次不相等，
                // 将日志转发器的term、leaderId与状态机同步，即发送compare请求。这种情况通常
                // 是由于集群触发了重新选举，当前节点刚被选举成 Leader节点。
                synchronized (memberState) {
                    if (!memberState.isLeader()) {
                        return true;
                    }
                    PreConditions.check(memberState.getSelfId().equals(memberState.getLeaderId()), DLedgerResponseCode.UNKNOWN);
                    logger.info("[Push-{}->{}]Update term: {} and leaderId: {} to new term: {}, new leaderId: {}", selfId, peerId, term, leaderId, memberState.currTerm(), memberState.getLeaderId());
                    term = memberState.currTerm();
                    leaderId = memberState.getSelfId();
                    // 改变日志转发器的状态，该方法非常重要
                    changeState(EntryDispatcherState.COMPARE);
                }
            }
            return false;
        }

        private PushEntryRequest buildCompareOrTruncatePushRequest(long preLogTerm, long preLogIndex,
            PushEntryRequest.Type type) {
            PushEntryRequest request = new PushEntryRequest();
            request.setGroup(memberState.getGroup());
            request.setRemoteId(peerId);
            request.setLeaderId(leaderId);
            // 设置节点ID
            request.setLocalId(memberState.getSelfId());
            // 设置所处选举轮次
            request.setTerm(term);
            request.setPreLogIndex(preLogIndex);
            request.setPreLogTerm(preLogTerm);
            request.setType(type);
            // 更新已提交的日志的索引
            request.setCommitIndex(memberState.getCommittedIndex());
            return request;
        }

        private PushEntryRequest buildCommitPushRequest() {
            PushEntryRequest request = new PushEntryRequest();
            request.setGroup(memberState.getGroup());
            request.setRemoteId(peerId);
            request.setLeaderId(leaderId);
            request.setLocalId(memberState.getSelfId());
            request.setTerm(term);
            request.setType(PushEntryRequest.Type.COMMIT);
            request.setCommitIndex(memberState.getCommittedIndex());
            return request;
        }

        private InstallSnapshotRequest buildInstallSnapshotRequest(DownloadSnapshot snapshot) {
            InstallSnapshotRequest request = new InstallSnapshotRequest();
            request.setGroup(memberState.getGroup());
            request.setRemoteId(peerId);
            request.setLeaderId(leaderId);
            request.setLocalId(memberState.getSelfId());
            request.setTerm(term);
            request.setLastIncludedIndex(snapshot.getMeta().getLastIncludedIndex());
            request.setLastIncludedTerm(snapshot.getMeta().getLastIncludedTerm());
            request.setData(snapshot.getData());
            return request;
        }

        private void resetBatchAppendEntryRequest() {
            batchAppendEntryRequest.setGroup(memberState.getGroup());
            batchAppendEntryRequest.setRemoteId(peerId);
            batchAppendEntryRequest.setLeaderId(leaderId);
            batchAppendEntryRequest.setLocalId(selfId);
            batchAppendEntryRequest.setTerm(term);
            batchAppendEntryRequest.setType(PushEntryRequest.Type.APPEND);
            batchAppendEntryRequest.clear();
        }

        private void checkQuotaAndWait(DLedgerEntry entry) {
            if (dLedgerStore.getLedgerEndIndex() - entry.getIndex() <= maxPendingSize) {
                return;
            }
            quota.sample(entry.getSize());
            if (quota.validateNow()) {
                long leftNow = quota.leftNow();
                logger.warn("[Push-{}]Quota exhaust, will sleep {}ms", peerId, leftNow);
                DLedgerUtils.sleep(leftNow);
            }
        }

        private DLedgerEntry getDLedgerEntryForAppend(long index) {
            DLedgerEntry entry;
            try {
                entry = dLedgerStore.get(index);
            } catch (DLedgerException e) {
                //  Do compare, in case the ledgerBeginIndex get refreshed.
                if (DLedgerResponseCode.INDEX_LESS_THAN_LOCAL_BEGIN.equals(e.getCode())) {
                    logger.info("[Push-{}]Get INDEX_LESS_THAN_LOCAL_BEGIN when requested index is {}, try to compare", peerId, index);
                    return null;
                }
                throw e;
            }
            PreConditions.check(entry != null, DLedgerResponseCode.UNKNOWN, "writeIndex=%d", index);
            return entry;
        }

        private void doCommit() throws Exception {
            if (DLedgerUtils.elapsed(lastPushCommitTimeMs) > 1000) {
                PushEntryRequest request = buildCommitPushRequest();
                //Ignore the results
                dLedgerRpcService.push(request);
                lastPushCommitTimeMs = System.currentTimeMillis();
            }
        }

        /**
         * Leader节点在向 从节点转发日志后，会存储该日志的推送时间戳到pendingMap，
         * 当pendingMap的积压超过1000ms时会触发重推机制，该逻辑封装在当前方法中
         * @throws Exception
         */
        private void doCheckAppendResponse() throws Exception {
            // 获取从节点已复制的日志序号
            long peerWaterMark = getPeerWaterMark(term, peerId);
            // 尝试获取从节点已复制序号+1的记录，如果能找到，说明从服务下一条需要追加的消息已经存储在主节点中，
            // 接着在尝试推送，如果该条推送已经超时，默认超时时间 为1s，调用doAppendInner重新推送
            Pair<Long, Integer> pair = pendingMap.get(peerWaterMark + 1);
            if (pair == null)
                return;
            long sendTimeMs = pair.getKey();
            if (DLedgerUtils.elapsed(sendTimeMs) > dLedgerConfig.getMaxPushTimeOutMs()) {
                // reset write index
                batchAppendEntryRequest.clear();
                writeIndex = peerWaterMark + 1;
                logger.warn("[Push-{}]Reset write index to {} for resending the entries which are timeout", peerId, peerWaterMark + 1);
            }
        }

        private synchronized void changeState(EntryDispatcherState target) {
            logger.info("[Push-{}]Change state from {} to {}, matchIndex: {}, writeIndex: {}", peerId, type.get(), target, matchIndex, writeIndex);
            switch (target) {
                case APPEND:
                    resetBatchAppendEntryRequest();
                    break;
                case COMPARE:
                    if (this.type.compareAndSet(EntryDispatcherState.APPEND, EntryDispatcherState.COMPARE)) {
                        writeIndex = dLedgerStore.getLedgerEndIndex() + 1;
                        pendingMap.clear();
                    }
                    break;
                default:
                    break;
            }
            type.set(target);
        }

        @Override
        public void doWork() {
            try {
                // 检查当前节点状态
                if (checkNotLeaderAndFreshState()) {
                    waitForRunning(1);
                    return;
                }
                switch (type.get()) {
                    case COMPARE:
                        doCompare();
                        break;
                    case TRUNCATE:
                        doTruncate();
                        break;
                    case APPEND:
                        doAppend();
                        break;
                    case INSTALL_SNAPSHOT:
                        doInstallSnapshot();
                        break;
                    case COMMIT:
                        doCommit();
                        break;
                }
                waitForRunning(1);
            } catch (Throwable t) {
                DLedgerEntryPusher.LOGGER.error("[Push-{}]Error in {} writeIndex={} matchIndex={}", peerId, getName(), writeIndex, matchIndex, t);
                changeState(EntryDispatcherState.COMPARE);
                DLedgerUtils.sleep(500);
            }
        }

        /**
         * 该方法用于Leader节点向从节点发送Compare请求，目的是为了找到与从节点的共识点，
         * 也就是找到从节点未提交的日志Index，从而实现删除从节点未提交的数据。
         *
         * @throws Exception
         */
        private void doCompare() throws Exception {
            // 注意这里是while(true)，所以需要注意循环退出条件
            while (true) {
                if (checkNotLeaderAndFreshState()) {
                    break;
                }
                // 判断请求类型是否为Compare，如果不是则退出循环
                if (this.type.get() != EntryDispatcherState.COMPARE) {
                    break;
                }
                // ledgerEndIndex== -1 表示Leader中没有存储数据，是一个新的集群，所以无需比较主从是否一致
                if (dLedgerStore.getLedgerEndIndex() == -1) {
                    break;
                }

                // compare process start from the [nextIndex -1]
                PushEntryRequest request;
                // compareIndex 代表正在比对的索引下标，对比前一条日志，term 和 index 是否一致
                long compareIndex = writeIndex - 1;
                long compareTerm = -1;
                if (compareIndex < dLedgerStore.getLedgerBeforeBeginIndex()) {
                    // 需要比较的条目已被压缩删除，只需更改状态即可安装快照
                    changeState(EntryDispatcherState.INSTALL_SNAPSHOT);
                    return;
                } else if (compareIndex == dLedgerStore.getLedgerBeforeBeginIndex()) {
                    compareTerm = dLedgerStore.getLedgerBeforeBeginTerm();
                    request = buildCompareOrTruncatePushRequest(compareTerm, compareIndex, PushEntryRequest.Type.COMPARE);
                } else {
                    // 获取正在比对的日志信息
                    DLedgerEntry entry = dLedgerStore.get(compareIndex);
                    PreConditions.check(entry != null, DLedgerResponseCode.INTERNAL_ERROR, "compareIndex=%d", compareIndex);
                    // 正在比对的日志所处的选举轮次
                    compareTerm = entry.getTerm();
                    request = buildCompareOrTruncatePushRequest(compareTerm, entry.getIndex(), PushEntryRequest.Type.COMPARE);
                }
                CompletableFuture<PushEntryResponse> responseFuture = dLedgerRpcService.push(request);
                PushEntryResponse response = responseFuture.get(3, TimeUnit.SECONDS);
                PreConditions.check(response != null, DLedgerResponseCode.INTERNAL_ERROR, "compareIndex=%d", compareIndex);
                PreConditions.check(response.getCode() == DLedgerResponseCode.INCONSISTENT_STATE.getCode() || response.getCode() == DLedgerResponseCode.SUCCESS.getCode()
                    , DLedgerResponseCode.valueOf(response.getCode()), "compareIndex=%d", compareIndex);

                // fast backup algorithm to locate the match index
                if (response.getCode() == DLedgerResponseCode.SUCCESS.getCode()) {
                    // 证明找到了与Follower节点的共识点
                    matchIndex = compareIndex;
                    // 此时更新这个Follower节点的水位线
                    updatePeerWaterMark(compareTerm, peerId, matchIndex);
                    // 将发送模式改成truncate，以将从节点的未提交的日志删除
                    changeState(EntryDispatcherState.TRUNCATE);
                    return;
                }

                // 证明在compareIndex日志上，Follower与当前Leader所处选举轮次并不一致，证明从节点这条日志是需要被删除，然后才会将主节点已提交的日志再次同步到follower上
                if (response.getXTerm() != -1) {
                    // response.getXTerm() != -1 代表当前对比index 所处的任期和Leader节点不一致，
                    // 此时 response.getXIndex() 返回的是当前对比任期在从节点结束的位置，所以将指针移到从节点在当前轮次的结束处，再次进行对比。
                    writeIndex = response.getXIndex();
                } else {
                    // response.getXTerm() == -1 代表从节点上的 leaderEndIndex 比当前对比的index小，
                    // 则把对比指针，移到从节点末尾的 leaderEndIndex上
                    writeIndex = response.getEndIndex() + 1;
                }
            }
        }

        /**
         * 发起truncate请求，用于删除Follower节点未提交的日志
         * @throws Exception
         */
        private void doTruncate() throws Exception {
            // 检测当前状态是否为Truncate
            PreConditions.check(type.get() == EntryDispatcherState.TRUNCATE, DLedgerResponseCode.UNKNOWN);
            // 删除共识点以后得所有日志，truncateIndex代表删除的起始位置
            long truncateIndex = matchIndex + 1;
            logger.info("[Push-{}]Will push data to truncate truncateIndex={}", peerId, truncateIndex);
            // 构建truncate请求
            PushEntryRequest truncateRequest = buildCompareOrTruncatePushRequest(-1, truncateIndex, PushEntryRequest.Type.TRUNCATE);
            // 发送请求，等待Follower响应
            PushEntryResponse truncateResponse = dLedgerRpcService.push(truncateRequest).get(3, TimeUnit.SECONDS);

            PreConditions.check(truncateResponse != null, DLedgerResponseCode.UNKNOWN, "truncateIndex=%d", truncateIndex);
            PreConditions.check(truncateResponse.getCode() == DLedgerResponseCode.SUCCESS.getCode(), DLedgerResponseCode.valueOf(truncateResponse.getCode()), "truncateIndex=%d", truncateIndex);
            // 更新 lastPushCommitTimeMs 时间
            lastPushCommitTimeMs = System.currentTimeMillis();
            // 将状态改为Append，Follower节点的多余日志删除完成后，就需要Leader节点同步数据给Follower了
            changeState(EntryDispatcherState.APPEND);
        }

        private void doAppend() throws Exception {
            while (true) {
                if (checkNotLeaderAndFreshState()) {
                    break;
                }
                // 校验当前状态是否是Append，如果不是则退出循环
                if (type.get() != EntryDispatcherState.APPEND) {
                    break;
                }
                // 检查从节点未接收的第一个append请求是否超时，如果超时，则重推
                doCheckAppendResponse();
                // check if now not new entries to be sent
                if (writeIndex > dLedgerStore.getLedgerEndIndex()) {
                    if (this.batchAppendEntryRequest.getCount() > 0) {
                        sendBatchAppendEntryRequest();
                    } else {
                        doCommit();
                    }
                    break;
                }
                // check if now not entries in store can be sent
                if (writeIndex <= dLedgerStore.getLedgerBeforeBeginIndex()) {
                    logger.info("[Push-{}]The ledgerBeginBeginIndex={} is less than or equal to  writeIndex={}", peerId, dLedgerStore.getLedgerBeforeBeginIndex(), writeIndex);
                    changeState(EntryDispatcherState.INSTALL_SNAPSHOT);
                    break;
                }
                if (pendingMap.size() >= maxPendingSize || DLedgerUtils.elapsed(lastCheckLeakTimeMs) > 1000) {
                    long peerWaterMark = getPeerWaterMark(term, peerId);
                    for (Map.Entry<Long, Pair<Long, Integer>> entry : pendingMap.entrySet()) {
                        if (entry.getKey() + entry.getValue().getValue() - 1 <= peerWaterMark) {
                            // clear the append request which all entries have been accepted in peer
                            pendingMap.remove(entry.getKey());
                        }
                    }
                    lastCheckLeakTimeMs = System.currentTimeMillis();
                }
                if (pendingMap.size() >= maxPendingSize) {
                    doCheckAppendResponse();
                    break;
                }
                // 循环同步数据至从节点，方法内部会优化，会按照配置收集一批需要发送的日志，等到到达发送阈值则一起发送，而不是一条条发送
                long lastIndexToBeSend = doAppendInner(writeIndex);
                if (lastIndexToBeSend == -1) {
                    break;
                }
                writeIndex = lastIndexToBeSend + 1;
            }
        }

        /**
         * append the entries to the follower, append it in memory until the threshold is reached, its will be really sent to peer
         *
         * @param index from which index to append
         * @return the index of the last entry to be appended
         * @throws Exception
         */
        private long doAppendInner(long index) throws Exception {
            DLedgerEntry entry = getDLedgerEntryForAppend(index);
            if (null == entry) {
                // means should install snapshot
                logger.error("[Push-{}]Get null entry from index={}", peerId, index);
                changeState(EntryDispatcherState.INSTALL_SNAPSHOT);
                return -1;
            }
            // check quota for flow controlling
            checkQuotaAndWait(entry);
            batchAppendEntryRequest.addEntry(entry);
            // check if now can trigger real send
            if (!dLedgerConfig.isEnableBatchAppend() || batchAppendEntryRequest.getTotalSize() >= dLedgerConfig.getMaxBatchAppendSize()
                || DLedgerUtils.elapsed(this.lastAppendEntryRequestSendTimeMs) >= dLedgerConfig.getMaxBatchAppendIntervalMs()) {
                // 开启了批量发送，并且到达了批量发送阈值
                sendBatchAppendEntryRequest();
            }
            return entry.getIndex();
        }

        private void sendBatchAppendEntryRequest() throws Exception {
            batchAppendEntryRequest.setCommitIndex(memberState.getCommittedIndex());
            final long firstIndex = batchAppendEntryRequest.getFirstEntryIndex();
            final long lastIndex = batchAppendEntryRequest.getLastEntryIndex();
            final long lastTerm = batchAppendEntryRequest.getLastEntryTerm();
            final long entriesCount = batchAppendEntryRequest.getCount();
            final long entriesSize = batchAppendEntryRequest.getTotalSize();
            StopWatch watch = StopWatch.createStarted();
            CompletableFuture<PushEntryResponse> responseFuture = dLedgerRpcService.push(batchAppendEntryRequest);
            pendingMap.put(firstIndex, new Pair<>(System.currentTimeMillis(), batchAppendEntryRequest.getCount()));
            responseFuture.whenComplete((x, ex) -> {
                try {
                    PreConditions.check(ex == null, DLedgerResponseCode.UNKNOWN);
                    DLedgerResponseCode responseCode = DLedgerResponseCode.valueOf(x.getCode());
                    switch (responseCode) {
                        case SUCCESS:
                            Attributes attributes = DLedgerMetricsManager.newAttributesBuilder().put(LABEL_REMOTE_ID, this.peerId).build();
                            DLedgerMetricsManager.replicateEntryLatency.record(watch.getTime(TimeUnit.MICROSECONDS), attributes);
                            DLedgerMetricsManager.replicateEntryBatchCount.record(entriesCount, attributes);
                            DLedgerMetricsManager.replicateEntryBatchBytes.record(entriesSize, attributes);
                            pendingMap.remove(firstIndex);
                            if (lastIndex > matchIndex) {
                                matchIndex = lastIndex;
                                updatePeerWaterMark(lastTerm, peerId, matchIndex);
                            }
                            break;
                        case INCONSISTENT_STATE:
                            logger.info("[Push-{}]Get INCONSISTENT_STATE when append entries from {} to {} when term is {}", peerId, firstIndex, lastIndex, term);
                            changeState(EntryDispatcherState.COMPARE);
                            break;
                        default:
                            logger.warn("[Push-{}]Get error response code {} {}", peerId, responseCode, x.baseInfo());
                            break;
                    }
                } catch (Throwable t) {
                    logger.error("Failed to deal with the callback when append request return", t);
                }
            });
            lastPushCommitTimeMs = System.currentTimeMillis();
            batchAppendEntryRequest.clear();
        }

        private void doInstallSnapshot() throws Exception {
            // get snapshot from snapshot manager
            if (checkNotLeaderAndFreshState()) {
                return;
            }
            if (type.get() != EntryDispatcherState.INSTALL_SNAPSHOT) {
                return;
            }
            if (fsmCaller.getSnapshotManager() == null) {
                logger.error("[DoInstallSnapshot-{}]snapshot mode is disabled", peerId);
                changeState(EntryDispatcherState.COMPARE);
                return;
            }
            SnapshotManager manager = fsmCaller.getSnapshotManager();
            SnapshotReader snpReader = manager.getSnapshotReaderIncludedTargetIndex(writeIndex);
            if (snpReader == null) {
                logger.error("[DoInstallSnapshot-{}]get latest snapshot whose lastIncludedIndex >= {}  failed", peerId, writeIndex);
                changeState(EntryDispatcherState.COMPARE);
                return;
            }
            DownloadSnapshot snapshot = snpReader.generateDownloadSnapshot();
            if (snapshot == null) {
                logger.error("[DoInstallSnapshot-{}]generate latest snapshot for download failed, index = {}", peerId, writeIndex);
                changeState(EntryDispatcherState.COMPARE);
                return;
            }
            long lastIncludedIndex = snapshot.getMeta().getLastIncludedIndex();
            long lastIncludedTerm = snapshot.getMeta().getLastIncludedTerm();
            InstallSnapshotRequest request = buildInstallSnapshotRequest(snapshot);
            StopWatch watch = StopWatch.createStarted();
            CompletableFuture<InstallSnapshotResponse> future = DLedgerEntryPusher.this.dLedgerRpcService.installSnapshot(request);
            InstallSnapshotResponse response = future.get(3, TimeUnit.SECONDS);
            PreConditions.check(response != null, DLedgerResponseCode.INTERNAL_ERROR, "installSnapshot lastIncludedIndex=%d", writeIndex);
            DLedgerResponseCode responseCode = DLedgerResponseCode.valueOf(response.getCode());
            switch (responseCode) {
                case SUCCESS:
                    Attributes attributes = DLedgerMetricsManager.newAttributesBuilder().put(LABEL_REMOTE_ID, this.peerId).build();
                    DLedgerMetricsManager.installSnapshotLatency.record(watch.getTime(TimeUnit.MICROSECONDS), attributes);
                    logger.info("[DoInstallSnapshot-{}]install snapshot success, lastIncludedIndex = {}, lastIncludedTerm", peerId, lastIncludedIndex, lastIncludedTerm);
                    if (lastIncludedIndex > matchIndex) {
                        matchIndex = lastIncludedIndex;
                        writeIndex = matchIndex + 1;
                    }
                    changeState(EntryDispatcherState.APPEND);
                    break;
                case INSTALL_SNAPSHOT_ERROR:
                case INCONSISTENT_STATE:
                    logger.info("[DoInstallSnapshot-{}]install snapshot failed, index = {}, term = {}", peerId, writeIndex, term);
                    changeState(EntryDispatcherState.COMPARE);
                    break;
                default:
                    logger.warn("[DoInstallSnapshot-{}]install snapshot failed because error response: code = {}, mas = {}, index = {}, term = {}", peerId, responseCode, response.baseInfo(), writeIndex, term);
                    changeState(EntryDispatcherState.COMPARE);
                    break;
            }
        }

    }

    enum EntryDispatcherState {
        /**
         * 如果Leader节点发生变化，新的Leader节点需要与它的从节点日志条目进行比较，以便截断从节点多余的数据。
         */
        COMPARE,
        /**
         * 如果Leader节点通过索引完成日志对比后，发现从节点存在多余的数据（未提交的数据），则Leader节点将发送 TRUNCATE给它的从节点，
         * 删除多余的数据，实现主从节点数据一致性。
         */
        TRUNCATE,
        /**
         * 将日志条目追加到从节点。
         */
        APPEND,
        INSTALL_SNAPSHOT,
        /**
         * 通常Leader节点会将提交的索引附加到append请求，如果append请求很少且分散，Leader节点将发送一个单独的请求来通 知从节点提交索引。
         */
        COMMIT
    }

    /**
     * 从节点收到Leader节点推送的日志并存储，然后向Leader节点汇报日志复制结果。
     * This thread will be activated by the follower.
     * Accept the push request and order it by the index, then append to ledger store one by one.
     */
    private class EntryHandler extends ShutdownAbleThread {

        /**
         * 上一次检查主服务器是否有推送消息的时间戳。
         */
        private long lastCheckFastForwardTimeMs = System.currentTimeMillis();

        /**
         * append请求处理队列。
         */
        ConcurrentMap<Long/*index*/, Pair<PushEntryRequest/*request*/, CompletableFuture<PushEntryResponse/*complete future*/>>> writeRequestMap = new ConcurrentHashMap<>();
        /**
         * COMMIT、COMPARE、TRUNCATE相关请求的处理队列。
         */
        BlockingQueue<Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>>
            compareOrTruncateRequests = new ArrayBlockingQueue<>(1024);

        private ReentrantLock inflightInstallSnapshotRequestLock = new ReentrantLock();

        private Pair<InstallSnapshotRequest, CompletableFuture<InstallSnapshotResponse>> inflightInstallSnapshotRequest;

        public EntryHandler(Logger logger) {
            super("EntryHandler-" + memberState.getSelfId(), logger);
        }

        public CompletableFuture<InstallSnapshotResponse> handleInstallSnapshot(InstallSnapshotRequest request) {
            CompletableFuture<InstallSnapshotResponse> future = new TimeoutFuture<>(1000);
            PreConditions.check(request.getData() != null && request.getData().length > 0, DLedgerResponseCode.UNEXPECTED_ARGUMENT);
            long index = request.getLastIncludedIndex();
            inflightInstallSnapshotRequestLock.lock();
            try {
                CompletableFuture<InstallSnapshotResponse> oldFuture = null;
                if (inflightInstallSnapshotRequest != null && inflightInstallSnapshotRequest.getKey().getLastIncludedIndex() >= index) {
                    oldFuture = future;
                    logger.warn("[MONITOR]The install snapshot request with index {} has already existed", index, inflightInstallSnapshotRequest.getKey());
                } else {
                    logger.warn("[MONITOR]The install snapshot request with index {} preempt inflight slot because of newer index", index);
                    if (inflightInstallSnapshotRequest != null && inflightInstallSnapshotRequest.getValue() != null) {
                        oldFuture = inflightInstallSnapshotRequest.getValue();
                    }
                    inflightInstallSnapshotRequest = new Pair<>(request, future);
                }
                if (oldFuture != null) {
                    InstallSnapshotResponse response = new InstallSnapshotResponse();
                    response.setGroup(request.getGroup());
                    response.setCode(DLedgerResponseCode.NEWER_INSTALL_SNAPSHOT_REQUEST_EXIST.getCode());
                    response.setTerm(request.getTerm());
                    oldFuture.complete(response);
                }
            } finally {
                inflightInstallSnapshotRequestLock.unlock();
            }
            return future;
        }

        public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
            // The timeout should smaller than the remoting layer's request timeout
            CompletableFuture<PushEntryResponse> future = new TimeoutFuture<>(1000);
            switch (request.getType()) {
                case APPEND:
                    PreConditions.check(request.getCount() > 0, DLedgerResponseCode.UNEXPECTED_ARGUMENT);
                    long index = request.getFirstEntryIndex();
                    // 将请求放入队列中，由doWork方法异步处理
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> old = writeRequestMap.putIfAbsent(index, new Pair<>(request, future));
                    if (old != null) {
                        // 表示重复推送
                        logger.warn("[MONITOR]The index {} has already existed with {} and curr is {}", index, old.getKey().baseInfo(), request.baseInfo());
                        future.complete(buildResponse(request, DLedgerResponseCode.REPEATED_PUSH.getCode()));
                    }
                    break;
                case COMMIT:
                    synchronized (this) {
                        // 将commit放入请求队列，由doWork方法异步处理
                        if (!compareOrTruncateRequests.offer(new Pair<>(request, future))) {
                            logger.warn("compareOrTruncateRequests blockingQueue is full when put commit request");
                            future.complete(buildResponse(request, DLedgerResponseCode.PUSH_REQUEST_IS_FULL.getCode()));
                        }
                    }
                    break;
                case COMPARE:
                case TRUNCATE:
                    // 如果是compare或truncate请求，则清除append队列中所有的请求
                    writeRequestMap.clear();
                    synchronized (this) {
                        // 并将 compare或truncate 请求放入队列中，由doWork方法异步处理
                        if (!compareOrTruncateRequests.offer(new Pair<>(request, future))) {
                            logger.warn("compareOrTruncateRequests blockingQueue is full when put compare or truncate request");
                            future.complete(buildResponse(request, DLedgerResponseCode.PUSH_REQUEST_IS_FULL.getCode()));
                        }
                    }
                    break;
                default:
                    logger.error("[BUG]Unknown type {} from {}", request.getType(), request.baseInfo());
                    future.complete(buildResponse(request, DLedgerResponseCode.UNEXPECTED_ARGUMENT.getCode()));
                    break;
            }
            wakeup();
            return future;
        }

        private PushEntryResponse buildResponse(PushEntryRequest request, int code) {
            PushEntryResponse response = new PushEntryResponse();
            response.setGroup(request.getGroup());
            response.setCode(code);
            response.setTerm(request.getTerm());
            if (request.getType() != PushEntryRequest.Type.COMMIT) {
                response.setIndex(request.getFirstEntryIndex());
                response.setCount(request.getCount());
            }
            return response;
        }

        private void handleDoAppend(long writeIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(writeIndex == request.getFirstEntryIndex(), DLedgerResponseCode.INCONSISTENT_STATE);
                for (DLedgerEntry entry : request.getEntries()) {
                    dLedgerStore.appendAsFollower(entry, request.getTerm(), request.getLeaderId());
                }
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                long committedIndex = Math.min(dLedgerStore.getLedgerEndIndex(), request.getCommitIndex());
                if (DLedgerEntryPusher.this.memberState.followerUpdateCommittedIndex(committedIndex)) {
                    DLedgerEntryPusher.this.fsmCaller.onCommitted(committedIndex);
                }
            } catch (Throwable t) {
                logger.error("[HandleDoAppend] writeIndex={}", writeIndex, t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
        }

        /**
         * Follower端处理Leader端发起的Compare请求
         *
         * @param request
         * @param future
         * @return
         */
        private CompletableFuture<PushEntryResponse> handleDoCompare(PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(request.getType() == PushEntryRequest.Type.COMPARE, DLedgerResponseCode.UNKNOWN);
                // Leader端发来需要对比的日志索引值
                long preLogIndex = request.getPreLogIndex();
                // Leader端Index日志所处的任期
                long preLogTerm = request.getPreLogTerm();
                if (preLogTerm == -1 && preLogIndex == -1) {
                    // leader节点日志为空，则直接返回
                    future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                    return future;
                }
                if (dLedgerStore.getLedgerEndIndex() >= preLogIndex) {
                    long compareTerm = 0;
                    // 找到指定Index在当前节点的日志中的任期
                    if (dLedgerStore.getLedgerBeforeBeginIndex() == preLogIndex) {
                        // 如果查找的Index刚好是当前节点存储的第一条日志，则不用读取磁盘获取日志任期
                        compareTerm = dLedgerStore.getLedgerBeforeBeginTerm();
                    } else {
                        // 从磁盘中读取日志内容，然后获取到日志任期
                        DLedgerEntry local = dLedgerStore.get(preLogIndex);
                        compareTerm = local.getTerm();
                    }
                    if (compareTerm == preLogTerm) {
                        // 如果任期相同，则认为Follower节点的日志和Leader节点是相同的，也就证明找到了共识点
                        future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                        return future;
                    }
                    // 如果任期不相同，则从preLogIndex开始，向前追溯compareTerm任期的第一个日志
                    DLedgerEntry firstEntryWithTargetTerm = dLedgerStore.getFirstLogOfTargetTerm(compareTerm, preLogIndex);
                    PreConditions.check(firstEntryWithTargetTerm != null, DLedgerResponseCode.INCONSISTENT_STATE);
                    PushEntryResponse response = buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode());
                    // 设置Leader节点对比的Index在当前节点所处的任期
                    response.setXTerm(compareTerm);
                    // 设置Leader节点对比任期，在当前节点最大的index值
                    response.setXIndex(firstEntryWithTargetTerm.getIndex());
                    future.complete(response);
                    return future;
                }
                // dLedgerStore.getLedgerEndIndex() < preLogIndex，代表Leader想要对比的日志在当前节点不存咋，则返回当前节点的endIndex
                PushEntryResponse response = buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode());
                response.setEndIndex(dLedgerStore.getLedgerEndIndex());
                future.complete(response);
            } catch (Throwable t) {
                logger.error("[HandleDoCompare] preLogIndex={}, preLogTerm={}", request.getPreLogIndex(), request.getPreLogTerm(), t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
            return future;
        }

        private CompletableFuture<PushEntryResponse> handleDoCommit(long committedIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(committedIndex == request.getCommitIndex(), DLedgerResponseCode.UNKNOWN);
                PreConditions.check(request.getType() == PushEntryRequest.Type.COMMIT, DLedgerResponseCode.UNKNOWN);
                committedIndex = committedIndex <= dLedgerStore.getLedgerEndIndex() ? committedIndex : dLedgerStore.getLedgerEndIndex();
                if (DLedgerEntryPusher.this.memberState.followerUpdateCommittedIndex(committedIndex)) {
                    DLedgerEntryPusher.this.fsmCaller.onCommitted(committedIndex);
                }
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
            } catch (Throwable t) {
                logger.error("[HandleDoCommit] committedIndex={}", request.getCommitIndex(), t);
                future.complete(buildResponse(request, DLedgerResponseCode.UNKNOWN.getCode()));
            }
            return future;
        }

        /**
         * 该方法时Follower节点收到Leader节点的Truncate请求所执行的方法
         * @param truncateIndex
         * @param request
         * @param future
         * @return
         */
        private CompletableFuture<PushEntryResponse> handleDoTruncate(long truncateIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                logger.info("[HandleDoTruncate] truncateIndex={}", truncateIndex);
                PreConditions.check(request.getType() == PushEntryRequest.Type.TRUNCATE, DLedgerResponseCode.UNKNOWN);
                // 删除truncateIndex之后的日志
                long index = dLedgerStore.truncate(truncateIndex);
                PreConditions.check(index == truncateIndex - 1, DLedgerResponseCode.INCONSISTENT_STATE);
                // 删除成功，则返回成功
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                // 更新本地的已提交索引，如果Leader已提交索引大于本地的最大索引，则证明本地的所有日志都处于已提交状态，反之则更新已提交索引为Leader的已提交索引
                long committedIndex = request.getCommitIndex() <= dLedgerStore.getLedgerEndIndex() ? request.getCommitIndex() : dLedgerStore.getLedgerEndIndex();
                // 更新状态机中的已提交索引
                if (DLedgerEntryPusher.this.memberState.followerUpdateCommittedIndex(committedIndex)) {
                    // todo 该方法待定
                    DLedgerEntryPusher.this.fsmCaller.onCommitted(committedIndex);
                }
            } catch (Throwable t) {
                logger.error("[HandleDoTruncate] truncateIndex={}", truncateIndex, t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
            return future;
        }

        private void handleDoInstallSnapshot(InstallSnapshotRequest request,
            CompletableFuture<InstallSnapshotResponse> future) {
            InstallSnapshotResponse response = new InstallSnapshotResponse();
            response.setGroup(request.getGroup());
            response.copyBaseInfo(request);
            try {
                logger.info("[HandleDoInstallSnapshot] begin to install snapshot, request={}", request);
                DownloadSnapshot snapshot = new DownloadSnapshot(new SnapshotMeta(request.getLastIncludedIndex(), request.getLastIncludedTerm()), request.getData());
                if (!fsmCaller.getSnapshotManager().installSnapshot(snapshot)) {
                    response.code(DLedgerResponseCode.INSTALL_SNAPSHOT_ERROR.getCode());
                    future.complete(response);
                    return;
                }
                response.code(DLedgerResponseCode.SUCCESS.getCode());
                future.complete(response);
            } catch (Throwable t) {
                logger.error("[HandleDoInstallSnapshot] install snapshot failed, request={}", request, t);
                response.code(DLedgerResponseCode.INSTALL_SNAPSHOT_ERROR.getCode());
                future.complete(response);
            }
        }

        /**
         *
         * @param endIndex 从节点当前存储的最大日志序号
         */
        private void checkAppendFuture(long endIndex) {
            long minFastForwardIndex = Long.MAX_VALUE;
            for (Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair : writeRequestMap.values()) {
                long firstEntryIndex = pair.getKey().getFirstEntryIndex();
                long lastEntryIndex = pair.getKey().getLastEntryIndex();
                // clear old push request
                if (lastEntryIndex <= endIndex) {
                    try {
                        for (DLedgerEntry dLedgerEntry : pair.getKey().getEntries()) {
                            PreConditions.check(dLedgerEntry.equals(dLedgerStore.get(dLedgerEntry.getIndex())), DLedgerResponseCode.INCONSISTENT_STATE);
                        }
                        pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.SUCCESS.getCode()));
                        logger.warn("[PushFallBehind]The leader pushed an batch append entry last index={} smaller than current ledgerEndIndex={}, maybe the last ack is missed", lastEntryIndex, endIndex);
                    } catch (Throwable t) {
                        logger.error("[PushFallBehind]The leader pushed an batch append entry last index={} smaller than current ledgerEndIndex={}, maybe the last ack is missed", lastEntryIndex, endIndex, t);
                        pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
                    }
                    writeRequestMap.remove(pair.getKey().getFirstEntryIndex());
                    continue;
                }
                // normal case
                if (firstEntryIndex == endIndex + 1) {
                    return;
                }
                // clear timeout push request
                TimeoutFuture<PushEntryResponse> future = (TimeoutFuture<PushEntryResponse>) pair.getValue();
                if (!future.isTimeOut()) {
                    continue;
                }
                if (firstEntryIndex < minFastForwardIndex) {
                    minFastForwardIndex = firstEntryIndex;
                }
            }
            if (minFastForwardIndex == Long.MAX_VALUE) {
                return;
            }
            Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = writeRequestMap.remove(minFastForwardIndex);
            if (pair == null) {
                return;
            }
            logger.warn("[PushFastForward] ledgerEndIndex={} entryIndex={}", endIndex, minFastForwardIndex);
            pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
        }

        /**
         * 检查追加请求是否丢失
         * The leader does push entries to follower, and record the pushed index. But in the following conditions, the push may get stopped.
         * * If the follower is abnormally shutdown, its ledger end index may be smaller than before. At this time, the leader may push fast-forward entries, and retry all the time.
         * * If the last ack is missed, and no new message is coming in.The leader may retry push the last message, but the follower will ignore it.
         *
         * @param endIndex
         */
        private void checkAbnormalFuture(long endIndex) {
            if (DLedgerUtils.elapsed(lastCheckFastForwardTimeMs) < 1000) {
                // 上次检查距离现在不足1s，则跳过检查
                return;
            }
            lastCheckFastForwardTimeMs = System.currentTimeMillis();
            if (writeRequestMap.isEmpty()) {
                // 当前没有积压的append的请求，可以证明主节点没有推送新的日志，所以不用检查
                return;
            }

            checkAppendFuture(endIndex);
        }

        private void clearCompareOrTruncateRequestsIfNeed() {
            synchronized (this) {
                if (!memberState.isFollower() && !compareOrTruncateRequests.isEmpty()) {
                    List<Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>> drainList = new ArrayList<>();
                    compareOrTruncateRequests.drainTo(drainList);
                    for (Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair : drainList) {
                        pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.NOT_FOLLOWER.getCode()));
                    }
                }
            }
        }

        @Override
        public void doWork() {
            try {
                // 校验是否是Follower节点
                if (!memberState.isFollower()) {
                    clearCompareOrTruncateRequestsIfNeed();
                    waitForRunning(1);
                    return;
                }
                // deal with install snapshot request first
                Pair<InstallSnapshotRequest, CompletableFuture<InstallSnapshotResponse>> installSnapshotPair = null;
                this.inflightInstallSnapshotRequestLock.lock();
                try {
                    if (inflightInstallSnapshotRequest != null && inflightInstallSnapshotRequest.getKey() != null && inflightInstallSnapshotRequest.getValue() != null) {
                        installSnapshotPair = inflightInstallSnapshotRequest;
                        inflightInstallSnapshotRequest = new Pair<>(null, null);
                    }
                } finally {
                    this.inflightInstallSnapshotRequestLock.unlock();
                }
                if (installSnapshotPair != null) {
                    handleDoInstallSnapshot(installSnapshotPair.getKey(), installSnapshotPair.getValue());
                }
                // deal with the compare or truncate requests
                if (compareOrTruncateRequests.peek() != null) {
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = compareOrTruncateRequests.poll();
                    PreConditions.check(pair != null, DLedgerResponseCode.UNKNOWN);
                    switch (pair.getKey().getType()) {
                        case TRUNCATE:
                            handleDoTruncate(pair.getKey().getPreLogIndex(), pair.getKey(), pair.getValue());
                            break;
                        case COMPARE:
                            handleDoCompare(pair.getKey(), pair.getValue());
                            break;
                        case COMMIT:
                            handleDoCommit(pair.getKey().getCommitIndex(), pair.getKey(), pair.getValue());
                            break;
                        default:
                            break;
                    }
                    return;
                }
                long nextIndex = dLedgerStore.getLedgerEndIndex() + 1;
                Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = writeRequestMap.remove(nextIndex);
                if (pair == null) {
                    // 检查追加请求是否丢失
                    checkAbnormalFuture(dLedgerStore.getLedgerEndIndex());
                    // 如果下一个日志索引不在队列中，则证明主节点还没有把这条日志推送过来，此时我们等待
                    waitForRunning(1);
                    return;
                }
                PushEntryRequest request = pair.getKey();
                handleDoAppend(nextIndex, request, pair.getValue());
            } catch (Throwable t) {
                DLedgerEntryPusher.LOGGER.error("Error in {}", getName(), t);
                DLedgerUtils.sleep(100);
            }
        }
    }
}
