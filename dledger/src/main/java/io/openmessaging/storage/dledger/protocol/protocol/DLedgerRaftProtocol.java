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

package io.openmessaging.storage.dledger.protocol.protocol;

import io.openmessaging.storage.dledger.protocol.HeartBeatRequest;
import io.openmessaging.storage.dledger.protocol.HeartBeatResponse;
import io.openmessaging.storage.dledger.protocol.InstallSnapshotRequest;
import io.openmessaging.storage.dledger.protocol.InstallSnapshotResponse;
import io.openmessaging.storage.dledger.protocol.PullEntriesRequest;
import io.openmessaging.storage.dledger.protocol.PullEntriesResponse;
import io.openmessaging.storage.dledger.protocol.PushEntryRequest;
import io.openmessaging.storage.dledger.protocol.PushEntryResponse;
import io.openmessaging.storage.dledger.protocol.VoteRequest;
import io.openmessaging.storage.dledger.protocol.VoteResponse;

import java.util.concurrent.CompletableFuture;

public interface DLedgerRaftProtocol {

    /**
     * 发起投票请求
     * @param request
     * @return
     * @throws Exception
     */
    CompletableFuture<VoteResponse> vote(VoteRequest request) throws Exception;

    /**
     * Leader节点向从节点发送心跳包。
     * @param request
     * @return
     * @throws Exception
     */
    CompletableFuture<HeartBeatResponse> heartBeat(HeartBeatRequest request) throws Exception;

    /**
     * 拉取日志条目
     * @param request
     * @return
     * @throws Exception
     */
    CompletableFuture<PullEntriesResponse> pull(PullEntriesRequest request) throws Exception;

    /**
     * 推送日志条目，用于日志传播
     * @param request
     * @return
     * @throws Exception
     */
    CompletableFuture<PushEntryResponse> push(PushEntryRequest request) throws Exception;

    /**
     * DLedger客户端协议处理器
     * @param request
     * @return
     * @throws Exception
     */
    CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request) throws Exception;

}
