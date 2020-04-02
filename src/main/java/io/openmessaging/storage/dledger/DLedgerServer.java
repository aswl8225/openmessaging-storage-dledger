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

package io.openmessaging.storage.dledger;

import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.exception.DLedgerException;
import io.openmessaging.storage.dledger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerProtocolHander;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.GetEntriesRequest;
import io.openmessaging.storage.dledger.protocol.GetEntriesResponse;
import io.openmessaging.storage.dledger.protocol.HeartBeatRequest;
import io.openmessaging.storage.dledger.protocol.HeartBeatResponse;
import io.openmessaging.storage.dledger.protocol.LeadershipTransferRequest;
import io.openmessaging.storage.dledger.protocol.LeadershipTransferResponse;
import io.openmessaging.storage.dledger.protocol.MetadataRequest;
import io.openmessaging.storage.dledger.protocol.MetadataResponse;
import io.openmessaging.storage.dledger.protocol.PullEntriesRequest;
import io.openmessaging.storage.dledger.protocol.PullEntriesResponse;
import io.openmessaging.storage.dledger.protocol.PushEntryRequest;
import io.openmessaging.storage.dledger.protocol.PushEntryResponse;
import io.openmessaging.storage.dledger.protocol.VoteRequest;
import io.openmessaging.storage.dledger.protocol.VoteResponse;
import io.openmessaging.storage.dledger.store.DLedgerMemoryStore;
import io.openmessaging.storage.dledger.store.DLedgerStore;
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import io.openmessaging.storage.dledger.utils.PreConditions;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLedgerServer implements DLedgerProtocolHander {

    private static Logger logger = LoggerFactory.getLogger(DLedgerServer.class);

    private MemberState memberState;
    private DLedgerConfig dLedgerConfig;

    private DLedgerStore dLedgerStore;
    private DLedgerRpcService dLedgerRpcService;
    private DLedgerEntryPusher dLedgerEntryPusher;
    private DLedgerLeaderElector dLedgerLeaderElector;

    private ScheduledExecutorService executorService;

    public DLedgerServer(DLedgerConfig dLedgerConfig) {
        this.dLedgerConfig = dLedgerConfig;

        /**
         * 初始化MemberState
         */
        this.memberState = new MemberState(dLedgerConfig);

        /**
         * 数据存储   内存或硬盘
         */
        this.dLedgerStore = createDLedgerStore(dLedgerConfig.getStoreType(), this.dLedgerConfig, this.memberState);

        /**
         * netty初始化
         */
        dLedgerRpcService = new DLedgerRpcNettyService(this);

        /**
         * 节点内部通讯及计算   实例化EntryDispatcher
         */
        dLedgerEntryPusher = new DLedgerEntryPusher(dLedgerConfig, memberState, dLedgerStore, dLedgerRpcService);

        /**
         * 选举leader
         */
        dLedgerLeaderElector = new DLedgerLeaderElector(dLedgerConfig, memberState, dLedgerRpcService);

        /**
         * 调度线程
         */
        executorService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("DLedgerServer-ScheduledExecutor");
            return t;
        });
    }


    public void startup() {
        /**
         * 文件系统
         */
        this.dLedgerStore.startup();

        /**
         * netty启动
         */
        this.dLedgerRpcService.startup();

        /**
         * commitlog同步
         */
        this.dLedgerEntryPusher.startup();

        /**
         * 选举
         */
        this.dLedgerLeaderElector.startup();

        /**
         * 检查优选leader
         */
        executorService.scheduleAtFixedRate(this::checkPreferredLeader, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        this.dLedgerLeaderElector.shutdown();
        this.dLedgerEntryPusher.shutdown();
        this.dLedgerRpcService.shutdown();
        this.dLedgerStore.shutdown();
        executorService.shutdown();
    }

    /**
     * 数据存储
     * @param storeType
     * @param config
     * @param memberState
     * @return
     */
    private DLedgerStore createDLedgerStore(String storeType, DLedgerConfig config, MemberState memberState) {
        if (storeType.equals(DLedgerConfig.MEMORY)) {
            //内存
            return new DLedgerMemoryStore(config, memberState);
        } else {
            //文件
            return new DLedgerMmapFileStore(config, memberState);
        }
    }

    public MemberState getMemberState() {
        return memberState;
    }

    /**
     * 接受leader的心跳
     * @param request
     * @return
     * @throws Exception
     */
    @Override public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception {
        try {
            /**
             * 该请求是发送给当前节点的  SelfId==RemoteId  并且同属于一个集群内部
             */
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());

            /**
             * 接受leader的心跳
             */
            return dLedgerLeaderElector.handleHeartBeat(request);
        } catch (DLedgerException e) {
            logger.error("[{}][HandleHeartBeat] failed", memberState.getSelfId(), e);
            HeartBeatResponse response = new HeartBeatResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }
    }

    /**
     * 接受其他节点的选举
     * @param request
     * @return
     * @throws Exception
     */
    @Override public CompletableFuture<VoteResponse> handleVote(VoteRequest request) throws Exception {
        try {
            /**
             * 该请求是发送给当前节点的  SelfId==RemoteId
             */
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());

            /**
             * 在同一个集群内部
             */
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());

            /**
             * 接受其他节点的选举  注意self为false
             */
            return dLedgerLeaderElector.handleVote(request, false);
        } catch (DLedgerException e) {
            logger.error("[{}][HandleVote] failed", memberState.getSelfId(), e);
            VoteResponse response = new VoteResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }
    }

    /**
     * Handle the append requests:
     * 1.append the entry to local store
     * 2.submit the future to entry pusher and wait the quorum ack
     * 3.if the pending requests are full, then reject it immediately
     *
     * @param request
     * @return
     * @throws IOException
     */
    @Override
    public CompletableFuture<AppendEntryResponse> handleAppend(AppendEntryRequest request) throws IOException {
        try {
            /**
             * 校验
             */
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            PreConditions.check(memberState.isLeader(), DLedgerResponseCode.NOT_LEADER);
            PreConditions.check(memberState.getTransferee() == null, DLedgerResponseCode.LEADER_TRANSFERRING);
            long currTerm = memberState.currTerm();
            if (dLedgerEntryPusher.isPendingFull(currTerm)) {
                /**
                 * Pending已满  返回错误
                 */
                AppendEntryResponse appendEntryResponse = new AppendEntryResponse();
                appendEntryResponse.setGroup(memberState.getGroup());
                appendEntryResponse.setCode(DLedgerResponseCode.LEADER_PENDING_FULL.getCode());
                appendEntryResponse.setTerm(currTerm);
                appendEntryResponse.setLeaderId(memberState.getSelfId());
                return AppendFuture.newCompletedFuture(-1, appendEntryResponse);
            } else {
                /**
                 * 存储数据
                 */
                DLedgerEntry dLedgerEntry = new DLedgerEntry();
                dLedgerEntry.setBody(request.getBody());
                /**
                 * leader写入data和index数据
                 */
                DLedgerEntry resEntry = dLedgerStore.appendAsLeader(dLedgerEntry);
                /**
                 * 等待集群中follower的ack
                 */
                return dLedgerEntryPusher.waitAck(resEntry);
            }
        } catch (DLedgerException e) {
            logger.error("[{}][HandleAppend] failed", memberState.getSelfId(), e);
            AppendEntryResponse response = new AppendEntryResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return AppendFuture.newCompletedFuture(-1, response);
        }
    }

    /**
     * 获取消息
     * @param request
     * @return
     * @throws IOException
     */
    @Override
    public CompletableFuture<GetEntriesResponse> handleGet(GetEntriesRequest request) throws IOException {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            PreConditions.check(memberState.isLeader(), DLedgerResponseCode.NOT_LEADER);
            /**
             * 获取消息
             */
            DLedgerEntry entry = dLedgerStore.get(request.getBeginIndex());
            GetEntriesResponse response = new GetEntriesResponse();
            response.setGroup(memberState.getGroup());
            if (entry != null) {
                response.setEntries(Collections.singletonList(entry));
            }
            return CompletableFuture.completedFuture(response);
        } catch (DLedgerException e) {
            logger.error("[{}][HandleGet] failed", memberState.getSelfId(), e);
            GetEntriesResponse response = new GetEntriesResponse();
            response.copyBaseInfo(request);
            response.setLeaderId(memberState.getLeaderId());
            response.setCode(e.getCode().getCode());
            return CompletableFuture.completedFuture(response);
        }
    }

    /**
     * 查询当前节点对应的leader
     * @param request
     * @return
     * @throws Exception
     */
    @Override public CompletableFuture<MetadataResponse> handleMetadata(MetadataRequest request) throws Exception {
        try {
            /**
             * 校验
             */
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());

            /**
             * 返回集群名称  集群中的成员   当前对应的leader
             */
            MetadataResponse metadataResponse = new MetadataResponse();
            metadataResponse.setGroup(memberState.getGroup());
            metadataResponse.setPeers(memberState.getPeerMap());
            metadataResponse.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(metadataResponse);
        } catch (DLedgerException e) {
            logger.error("[{}][HandleMetadata] failed", memberState.getSelfId(), e);
            MetadataResponse response = new MetadataResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }

    }

    @Override
    public CompletableFuture<PullEntriesResponse> handlePull(PullEntriesRequest request) {
        return null;
    }

    /**
     * follower接受leader推送得消息
     * @param request
     * @return
     * @throws Exception
     */
    @Override public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());

            /**
             * follower接受leader推送得消息
             */
            return dLedgerEntryPusher.handlePush(request);
        } catch (DLedgerException e) {
            logger.error("[{}][HandlePush] failed", memberState.getSelfId(), e);
            PushEntryResponse response = new PushEntryResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }

    }

    /**
     * 优选节点处理leader发送得主转让通知
     * @param request
     * @return
     * @throws Exception
     */
    @Override
    public CompletableFuture<LeadershipTransferResponse> handleLeadershipTransfer(LeadershipTransferRequest request) throws Exception {
        try {
            /**
             * 校验
             */
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());

            /**
             * leader收到主转让通知
             */
            if (memberState.getSelfId().equals(request.getTransferId())) {
                //It's the leader received the transfer command.
                PreConditions.check(memberState.isPeerMember(request.getTransfereeId()), DLedgerResponseCode.UNKNOWN_MEMBER, "transferee=%s is not a peer member", request.getTransfereeId());
                PreConditions.check(memberState.currTerm() == request.getTerm(), DLedgerResponseCode.INCONSISTENT_TERM, "currTerm(%s) != request.term(%s)", memberState.currTerm(), request.getTerm());
                PreConditions.check(memberState.isLeader(), DLedgerResponseCode.NOT_LEADER, "selfId=%s is not leader=%s", memberState.getSelfId(), memberState.getLeaderId());

                // check fall transferee not fall behind much.
                long transfereeFallBehind = dLedgerStore.getLedgerEndIndex() - dLedgerEntryPusher.getPeerWaterMark(request.getTerm(), request.getTransfereeId());
                PreConditions.check(transfereeFallBehind < dLedgerConfig.getMaxLeadershipTransferWaitIndex(),
                    DLedgerResponseCode.FALL_BEHIND_TOO_MUCH, "transferee fall behind too much, diff=%s", transfereeFallBehind);
                return dLedgerLeaderElector.handleLeadershipTransfer(request);
            } else if (memberState.getSelfId().equals(request.getTransfereeId())) {
                /**
                 * 优选节点接到leader节点得主转让通知
                 */
                // It's the transferee received the take leadership command.
                PreConditions.check(request.getTransferId().equals(memberState.getLeaderId()), DLedgerResponseCode.INCONSISTENT_LEADER, "transfer=%s is not leader", request.getTransferId());

                return dLedgerLeaderElector.handleTakeLeadership(request);
            } else {
                return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.UNEXPECTED_ARGUMENT.getCode()));
            }
        } catch (DLedgerException e) {
            logger.error("[{}][handleLeadershipTransfer] failed", memberState.getSelfId(), e);
            LeadershipTransferResponse response = new LeadershipTransferResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }

    }

    /**
     * 检查优选节点
     */
    private void checkPreferredLeader() {
        /**
         * 非leader则跳出
         */
        if (!memberState.isLeader()) {
            return;
        }
        //优选节点
        String preferredLeaderId = dLedgerConfig.getPreferredLeaderId();

        /**
         * 无优选节点   或  当前leader本身就是优选节点   则跳出
         */
        if (preferredLeaderId == null || preferredLeaderId.equals(dLedgerConfig.getSelfId())) {
            return;
        }

        /**
         * 优选节点非集群内节点  则跳出
         */
        if (!memberState.isPeerMember(preferredLeaderId)) {
            logger.warn("preferredLeaderId = {} is not a peer member", preferredLeaderId);
            return;
        }

        /**
         * 已经开始主装让交易
         */
        if (memberState.getTransferee() != null) {
            return;
        }

        /**
         * 优选节点不可用
         */
        if (!memberState.getPeersLiveTable().containsKey(preferredLeaderId) ||
            memberState.getPeersLiveTable().get(preferredLeaderId) == Boolean.FALSE) {
            logger.warn("preferredLeaderId = {} is not online", preferredLeaderId);
            return;
        }

        /**
         * leader和优选节点间   数据同步得差值（即优选节点还有多少数据待同步）
         */
        long fallBehind = dLedgerStore.getLedgerEndIndex() - dLedgerEntryPusher.getPeerWaterMark(memberState.currTerm(), preferredLeaderId);
        logger.info("transferee fall behind index : {}", fallBehind);

        /**
         * 数据差小于阈值   启动主装让
         */
        if (fallBehind < dLedgerConfig.getMaxLeadershipTransferWaitIndex()) {
            LeadershipTransferRequest request = new LeadershipTransferRequest();
            request.setTerm(memberState.currTerm());
            request.setTransfereeId(dLedgerConfig.getPreferredLeaderId());

            try {
                long startTransferTime = System.currentTimeMillis();
                /**
                 * 主装让
                 */
                LeadershipTransferResponse response = dLedgerLeaderElector.handleLeadershipTransfer(request).get();
                logger.info("transfer finished. request={},response={},cost={}ms", request, response, DLedgerUtils.elapsed(startTransferTime));
            } catch (Throwable t) {
                logger.error("[checkPreferredLeader] error, request={}", request, t);
            }
        }
    }

    public DLedgerStore getdLedgerStore() {
        return dLedgerStore;
    }

    public DLedgerRpcService getdLedgerRpcService() {
        return dLedgerRpcService;
    }

    public DLedgerLeaderElector getdLedgerLeaderElector() {
        return dLedgerLeaderElector;
    }

    public DLedgerConfig getdLedgerConfig() {
        return dLedgerConfig;
    }
}
