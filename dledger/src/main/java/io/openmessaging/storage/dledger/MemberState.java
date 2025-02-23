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

import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.utils.IOUtils;
import io.openmessaging.storage.dledger.utils.PreConditions;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.openmessaging.storage.dledger.MemberState.Role.CANDIDATE;
import static io.openmessaging.storage.dledger.MemberState.Role.FOLLOWER;
import static io.openmessaging.storage.dledger.MemberState.Role.LEADER;

/**
 * 节点状态机，即Raft协议中Follower、Candidate、Leader三种状态的状态机实现
 */
public class MemberState {

    public static final String TERM_PERSIST_FILE = "currterm";
    public static final String TERM_PERSIST_KEY_TERM = "currTerm";
    public static final String TERM_PERSIST_KEY_VOTE_FOR = "voteLeader";

    public static Logger logger = LoggerFactory.getLogger(MemberState.class);

    private final ReentrantLock defaultLock = new ReentrantLock();

    // basic cluster info
    public final DLedgerConfig dLedgerConfig;
    private final String group;
    private final String selfId;
    private final String peers;
    private final Map<String, String> peerMap = new HashMap<>();
    private final Map<String, Boolean> peersLiveTable = new ConcurrentHashMap<>();

    // volatile states for all servers
    private volatile Role role = CANDIDATE;
    private volatile String leaderId;
    private volatile long committedIndex = -1;
    private volatile long appliedIndex = -1;

    private volatile long appliedTerm = -1;

    // persistent states for all servers
    private volatile long currTerm = 0;
    private volatile String currVoteFor;

    private volatile long ledgerEndIndex = -1;
    private volatile long ledgerEndTerm = -1;
    private long knownMaxTermInGroup = -1;


    // state for leadership transfer
    private volatile String transferee;
    private volatile long termToTakeLeadership = -1;

    public MemberState(DLedgerConfig config) {
        this.group = config.getGroup();
        this.selfId = config.getSelfId();
        this.peers = config.getPeers();
        for (String peerInfo : this.peers.split(";")) {
            String peerSelfId = peerInfo.split("-")[0];
            String peerAddress = peerInfo.substring(peerSelfId.length() + 1);
            peerMap.put(peerSelfId, peerAddress);
        }
        this.dLedgerConfig = config;
        loadPersistentState();
    }

    private void loadPersistentState() {
        try {
            String data = IOUtils.file2String(dLedgerConfig.getDefaultPath() + File.separator + TERM_PERSIST_FILE);
            Properties properties = IOUtils.string2Properties(data);
            if (properties == null) {
                return;
            }
            if (properties.containsKey(TERM_PERSIST_KEY_TERM)) {
                currTerm = Long.parseLong(String.valueOf(properties.get(TERM_PERSIST_KEY_TERM)));
            }
            if (properties.containsKey(TERM_PERSIST_KEY_VOTE_FOR)) {
                currVoteFor = String.valueOf(properties.get(TERM_PERSIST_KEY_VOTE_FOR));
                if (currVoteFor.length() == 0) {
                    currVoteFor = null;
                }
            }
            logger.info("[MemberState] load persistent state ok. term={} voteFor={}", currTerm, currVoteFor);
        } catch (Throwable t) {
            logger.error("Load last term failed", t);
        }
    }

    private void persistState() {
        try {
            Properties properties = new Properties();
            properties.put(TERM_PERSIST_KEY_TERM, currTerm);
            properties.put(TERM_PERSIST_KEY_VOTE_FOR, currVoteFor == null ? "" : currVoteFor);
            String data = IOUtils.properties2String(properties);
            IOUtils.string2File(data, dLedgerConfig.getDefaultPath() + File.separator + TERM_PERSIST_FILE);
        } catch (Throwable t) {
            logger.error("Persist curr term failed", t);
        }
    }

    public long currTerm() {
        return currTerm;
    }

    public String currVoteFor() {
        return currVoteFor;
    }

    public synchronized void setCurrVoteFor(String currVoteFor) {
        this.currVoteFor = currVoteFor;
        persistState();
    }

    public synchronized long nextTerm() {
        PreConditions.check(role == CANDIDATE, DLedgerResponseCode.ILLEGAL_MEMBER_STATE, "%s != %s", role, CANDIDATE);
        if (knownMaxTermInGroup > currTerm) {
            currTerm = knownMaxTermInGroup;
        } else {
            ++currTerm;
        }
        currVoteFor = null;
        persistState();
        return currTerm;
    }

    public synchronized void changeToLeader(long term) {
        PreConditions.check(currTerm == term, DLedgerResponseCode.ILLEGAL_MEMBER_STATE, "%d != %d", currTerm, term);
        this.role = LEADER;
        this.leaderId = selfId;
        peersLiveTable.clear();
    }

    public synchronized void changeToFollower(long term, String leaderId) {
        PreConditions.check(currTerm == term, DLedgerResponseCode.ILLEGAL_MEMBER_STATE, "%d != %d", currTerm, term);
        this.role = FOLLOWER;
        this.leaderId = leaderId;
        transferee = null;
    }

    public synchronized void changeToCandidate(long term) {
        assert term >= currTerm;
        PreConditions.check(term >= currTerm, DLedgerResponseCode.ILLEGAL_MEMBER_STATE, "should %d >= %d", term, currTerm);
        if (term > knownMaxTermInGroup) {
            knownMaxTermInGroup = term;
        }
        //the currTerm should be promoted in handleVote thread
        this.role = CANDIDATE;
        this.leaderId = null;
        transferee = null;
    }

    public String getTransferee() {
        return transferee;
    }

    public void setTransferee(String transferee) {
        PreConditions.check(role == LEADER, DLedgerResponseCode.ILLEGAL_MEMBER_STATE, "%s is not leader", selfId);
        this.transferee = transferee;
    }

    public long getTermToTakeLeadership() {
        return termToTakeLeadership;
    }

    public void setTermToTakeLeadership(long termToTakeLeadership) {
        this.termToTakeLeadership = termToTakeLeadership;
    }

    public String getSelfId() {
        return selfId;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public String getGroup() {
        return group;
    }

    public String getSelfAddr() {
        return peerMap.get(selfId);
    }

    public String getLeaderAddr() {
        return peerMap.get(leaderId);
    }

    public String getPeerAddr(String peerId) {
        return peerMap.get(peerId);
    }

    public boolean isLeader() {
        return role == LEADER;
    }

    public boolean isFollower() {
        return role == FOLLOWER;
    }

    public boolean isCandidate() {
        return role == CANDIDATE;
    }

    public boolean isQuorum(int num) {
        return num >= ((peerSize() / 2) + 1);
    }

    public int peerSize() {
        return peerMap.size();
    }

    public boolean isPeerMember(String id) {
        return id != null && peerMap.containsKey(id);
    }

    public Map<String, String> getPeerMap() {
        return peerMap;
    }

    public Map<String, Boolean> getPeersLiveTable() {
        return peersLiveTable;
    }

    //just for test
    public void setCurrTermForTest(long term) {
        PreConditions.check(term >= currTerm, DLedgerResponseCode.ILLEGAL_MEMBER_STATE);
        this.currTerm = term;
    }

    public Role getRole() {
        return role;
    }

    public ReentrantLock getDefaultLock() {
        return defaultLock;
    }

    public void updateLedgerIndexAndTerm(long index, long term) {
        this.ledgerEndIndex = index;
        this.ledgerEndTerm = term;
    }

    public long getLedgerEndIndex() {
        return ledgerEndIndex;
    }

    public long getLedgerEndTerm() {
        return ledgerEndTerm;
    }

    public long getCommittedIndex() {
        return committedIndex;
    }

    /**
     * update committedIndex
     *
     * @param term term of the entry which is committed. Very importantly, we can only commit the entry with current term, which also means that
     *             we can't commit the entry with old term.
     * @param committedIndex the index of the entry which is committed.
     * @return true if the committedIndex is updated, otherwise false.
     */
    public boolean leaderUpdateCommittedIndex(long term, long committedIndex) {
        if (!this.isLeader()) return false;
        // prevent back to an old state
        if (term < this.currTerm || committedIndex <= this.committedIndex) {
            return false;
        }
        logger.debug("[MemberState] leader update committed index from {} to {}", this.committedIndex, committedIndex);
        // 更新已提交的的commitIndex索引位置
        this.committedIndex = committedIndex;
        return true;
    }

    public boolean followerUpdateCommittedIndex(long committedIndex) {
        if (this.isLeader()) return false;
        if (committedIndex <= this.committedIndex) {
            return false;
        }
        logger.debug("[MemberState] follower update committed index from {} to {}", this.committedIndex, committedIndex);
        this.committedIndex = committedIndex;
        return true;
    }

    public long getAppliedIndex() {
        return appliedIndex;
    }

    public long getAppliedTerm() {
        return appliedTerm;
    }

    public void updateAppliedIndexAndTerm(long applyIndex, long appliedTerm) {
        if (appliedTerm < this.appliedTerm || applyIndex <= this.appliedIndex) {
            return;
        }
        this.appliedIndex = applyIndex;
        this.appliedTerm = appliedTerm;
    }

    public enum Role {
        UNKNOWN,
        CANDIDATE,
        LEADER,
        FOLLOWER
    }
}
