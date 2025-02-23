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

import io.openmessaging.storage.dledger.protocol.handler.DLedgerRpcProtocolHandler;
import io.openmessaging.storage.dledger.protocol.userdefine.UserDefineProcessor;
import io.openmessaging.storage.dledger.protocol.protocol.DLedgerProtocol;
import io.openmessaging.storage.dledger.protocol.userdefine.UserDefineRequest;
import io.openmessaging.storage.dledger.protocol.userdefine.UserDefineResponse;

/**
 * DLedger节点之前的网络通信，默认基于Netty实现，默认实现类为DLedgerRpcNettyService
 */
public abstract class DLedgerRpcService implements DLedgerProtocol, DLedgerRpcProtocolHandler {

    public abstract void startup();

    public abstract void shutdown();

    public abstract void registerUserDefineProcessor(UserDefineProcessor<? extends UserDefineRequest, ? extends UserDefineResponse> userDefineProcessor);

}
