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

package io.openmessaging.storage.dledger.store;

import io.openmessaging.storage.dledger.MemberState;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;

/**
 * 存储抽象类
 */
public abstract class DLedgerStore {

    /**
     * 获取节点状态机
     * @return
     */
    public abstract MemberState getMemberState();

    /**
     * 向主节点追加日志（数据）
     * @param entry
     * @return
     */
    public abstract DLedgerEntry appendAsLeader(DLedgerEntry entry);

    /**
     * 向从节点广播日志（数据）
     * @param entry
     * @param leaderTerm
     * @param leaderId
     * @return
     */
    public abstract DLedgerEntry appendAsFollower(DLedgerEntry entry, long leaderTerm, String leaderId);

    /**
     * 根据日志下标查找日志
     * @param index
     * @return
     */
    public abstract DLedgerEntry get(Long index);

    /**
     * 获取Leader节点当前最大的投票轮次
     * @return
     */
    public abstract long getLedgerEndTerm();

    /**
     * 获取Leader节点下一条日志写入的日志序号
     * @return
     */
    public abstract long getLedgerEndIndex();

    /**
     * 获取当前节点日志开始的日志序号
     * @return
     */
    public abstract long getLedgerBeforeBeginIndex();

    /**
     * 获取当前节点日志开始的投票轮次
     * @return
     */
    public abstract long getLedgerBeforeBeginTerm();

    /**
     * 更新Leader 节点维护的ledgerEndIndex和ledgerEndTerm
     */
    protected void updateLedgerEndIndexAndTerm() {
        if (getMemberState() != null) {
            getMemberState().updateLedgerIndexAndTerm(getLedgerEndIndex(), getLedgerEndTerm());
        }
    }

    /**
     * 刷盘
     */
    public abstract void flush();

    /**
     * 删除日志
     * @param entry
     * @param leaderTerm
     * @param leaderId
     * @return
     */
    public long truncate(DLedgerEntry entry, long leaderTerm, String leaderId) {
        return -1;
    }

    /**
     * 删除日志
     * truncate all entries in [truncateIndex ..]
     * @param truncateIndex truncate process since where
     * @return after truncate, store's end index
     */
    public abstract long truncate(long truncateIndex);

    /**
     * reset store's first entry, clear all entries in [.. beforeBeginIndex], make beforeBeginIndex + 1 to be first entry's index
     * @param beforeBeginIndex after reset process, beforeBegin entry's index
     * @param beforeBeginTerm after reset process, beforeBegin entry's  term
     * @return after reset, store's first log index
     */
    public abstract long reset(long beforeBeginIndex, long beforeBeginTerm);

    public abstract void resetOffsetAfterSnapshot(DLedgerEntry entry);

    public abstract void updateIndexAfterLoadingSnapshot(long lastIncludedIndex, long lastIncludedTerm);

    /**
     * 从endIndex开始，向前追溯targetTerm任期的第一个日志
     * @param targetTerm
     * @param endIndex
     * @return
     */
    public abstract DLedgerEntry getFirstLogOfTargetTerm(long targetTerm, long endIndex);

    /**
     * 启动存储管理器
     */
    public abstract void startup();

    /**
     * 关闭存储管理器
     */
    public abstract void shutdown();

}
