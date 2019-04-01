/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.client;

import io.openmessaging.storage.dledger.ShutdownAbleThread;
import io.openmessaging.storage.dledger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.GetEntriesRequest;
import io.openmessaging.storage.dledger.protocol.GetEntriesResponse;
import io.openmessaging.storage.dledger.protocol.MetadataRequest;
import io.openmessaging.storage.dledger.protocol.MetadataResponse;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLedgerClient {

    private static Logger logger = LoggerFactory.getLogger(DLedgerClient.class);
    //集群内成员
    private final Map<String, String> peerMap = new ConcurrentHashMap<>();
    //集群名称
    private final String group;
    //leader
    private String leaderId;
    private DLedgerClientRpcService dLedgerClientRpcService;

    private MetadataUpdater metadataUpdater = new MetadataUpdater("MetadataUpdater", logger);

    /**
     * DLedger客户端
     * @param group
     * @param peers
     */
    public DLedgerClient(String group, String peers) {
        this.group = group;
        updatePeers(peers);
        dLedgerClientRpcService = new DLedgerClientRpcNettyService();
        dLedgerClientRpcService.updatePeers(peers);
        /**
         * 集群内的第一个成员   做为leader
         */
        leaderId = peerMap.keySet().iterator().next();
    }

    public AppendEntryResponse append(byte[] body) {
        try {
            /**
             * 获取leader
             */
            waitOnUpdatingMetadata(1500, false);
            if (leaderId == null) {
                AppendEntryResponse appendEntryResponse = new AppendEntryResponse();
                appendEntryResponse.setCode(DLedgerResponseCode.METADATA_ERROR.getCode());
                return appendEntryResponse;
            }

            AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
            appendEntryRequest.setGroup(group);
            /**
             * 只向leader写入数据
             */
            appendEntryRequest.setRemoteId(leaderId);
            appendEntryRequest.setBody(body);
            /**
             * 写入数据
             */
            AppendEntryResponse response = dLedgerClientRpcService.append(appendEntryRequest).get();
            if (response.getCode() == DLedgerResponseCode.NOT_LEADER.getCode()) {
                waitOnUpdatingMetadata(1500, true);
                if (leaderId != null) {
                    appendEntryRequest.setRemoteId(leaderId);
                    response = dLedgerClientRpcService.append(appendEntryRequest).get();
                }
            }
            return response;
        } catch (Exception e) {
            needFreshMetadata();
            logger.error("{}", e);
            AppendEntryResponse appendEntryResponse = new AppendEntryResponse();
            appendEntryResponse.setCode(DLedgerResponseCode.INTERNAL_ERROR.getCode());
            return appendEntryResponse;
        }
    }

    public GetEntriesResponse get(long index) {
        try {
            waitOnUpdatingMetadata(1500, false);
            if (leaderId == null) {
                GetEntriesResponse response = new GetEntriesResponse();
                response.setCode(DLedgerResponseCode.METADATA_ERROR.getCode());
                return response;
            }

            GetEntriesRequest request = new GetEntriesRequest();
            request.setGroup(group);
            request.setRemoteId(leaderId);
            request.setBeginIndex(index);
            GetEntriesResponse response = dLedgerClientRpcService.get(request).get();
            if (response.getCode() == DLedgerResponseCode.NOT_LEADER.getCode()) {
                waitOnUpdatingMetadata(1500, true);
                if (leaderId != null) {
                    request.setRemoteId(leaderId);
                    response = dLedgerClientRpcService.get(request).get();
                }
            }
            return response;
        } catch (Exception t) {
            needFreshMetadata();
            logger.error("", t);
            GetEntriesResponse getEntriesResponse = new GetEntriesResponse();
            getEntriesResponse.setCode(DLedgerResponseCode.INTERNAL_ERROR.getCode());
            return getEntriesResponse;
        }
    }

    public void startup() {
        /**
         * 启动netty
         */
        this.dLedgerClientRpcService.startup();

        /**
         * 查询leader
         */
        this.metadataUpdater.start();
    }

    public void shutdown() {
        this.dLedgerClientRpcService.shutdown();
        this.metadataUpdater.shutdown();
    }

    private void updatePeers(String peers) {
        for (String peerInfo : peers.split(";")) {
            peerMap.put(peerInfo.split("-")[0], peerInfo.split("-")[1]);
        }
    }

    /**
     * 设置客户端的leader为null   并唤醒metadataUpdater   查询集群中的leader
     */
    private synchronized void needFreshMetadata() {
        leaderId = null;
        metadataUpdater.wakeup();
    }

    /**
     * 客户端是否获得当前集群中的leader
     * @param maxWaitMs
     * @param needFresh  重新获取leader
     */
    private synchronized void waitOnUpdatingMetadata(long maxWaitMs, boolean needFresh) {
        if (needFresh) {
            leaderId = null;
        } else if (leaderId != null) {
            return;
        }
        long start = System.currentTimeMillis();
        while (DLedgerUtils.elapsed(start) < maxWaitMs && leaderId == null) {
            /**
             * 唤醒metadataUpdater
             */
            metadataUpdater.wakeup();
            try {
                wait(1000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private class MetadataUpdater extends ShutdownAbleThread {

        public MetadataUpdater(String name, Logger logger) {
            super(name, logger);
        }

        /**
         * 向peerId查询当前集群中的leader
         * @param peerId
         * @param isLeader
         */
        private void getMetadata(String peerId, boolean isLeader) {
            try {
                MetadataRequest request = new MetadataRequest();
                request.setGroup(group);
                request.setRemoteId(peerId);
                /**
                 * 发起查询
                 */
                CompletableFuture<MetadataResponse> future = dLedgerClientRpcService.metadata(request);
                MetadataResponse response = future.get(1500, TimeUnit.MILLISECONDS);
                if (response.getLeaderId() != null) {
                    /**
                     * 更新leader
                     */
                    leaderId = response.getLeaderId();
                    if (response.getPeers() != null) {
                        /**
                         * 更新集群成员
                         */
                        peerMap.putAll(response.getPeers());
                        dLedgerClientRpcService.updatePeers(response.getPeers());
                    }
                }
            } catch (Throwable t) {
                if (isLeader) {
                    /**
                     * 查询集群中的leader
                     */
                    needFreshMetadata();
                }
                logger.warn("Get metadata failed from {}", peerId, t);
            }

        }

        @Override public void doWork() {
            try {
                if (leaderId == null) {
                    /**
                     * 没有leader   则向集群中的成员分别发送请求
                     */
                    for (String peer : peerMap.keySet()) {
                        getMetadata(peer, false);
                        if (leaderId != null) {
                            synchronized (DLedgerClient.this) {
                                DLedgerClient.this.notifyAll();
                            }
                            DLedgerUtils.sleep(1000);
                            break;
                        }
                    }
                } else {
                    /**
                     * 已知leader  只向leader查询确认
                     */
                    getMetadata(leaderId, true);
                }
                waitForRunning(3000);
            } catch (Throwable t) {
                logger.error("Error", t);
                DLedgerUtils.sleep(1000);
            }
        }
    }

}
