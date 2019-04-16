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

import com.alibaba.fastjson.JSON;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.PushEntryRequest;
import io.openmessaging.storage.dledger.protocol.PushEntryResponse;
import io.openmessaging.storage.dledger.store.DLedgerMemoryStore;
import io.openmessaging.storage.dledger.store.DLedgerStore;
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import io.openmessaging.storage.dledger.utils.Pair;
import io.openmessaging.storage.dledger.utils.PreConditions;
import io.openmessaging.storage.dledger.utils.Quota;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLedgerEntryPusher {

    private static Logger logger = LoggerFactory.getLogger(DLedgerEntryPusher.class);

    private DLedgerConfig dLedgerConfig;
    private DLedgerStore dLedgerStore;

    private final MemberState memberState;

    private DLedgerRpcService dLedgerRpcService;

    /**
     * Map<term, ConcurrentMap<peerId, index>>
     * 当前term下   peerId存储的最后消息的index
     */
    private Map<Long, ConcurrentMap<String, Long>> peerWaterMarksByTerm = new ConcurrentHashMap<>();
    /**Map<term, ConcurrentMap<index, TimeoutFuture<AppendEntryResponse>>>**/
    private Map<Long, ConcurrentMap<Long, TimeoutFuture<AppendEntryResponse>>> pendingAppendResponsesByTerm = new ConcurrentHashMap<>();

    private EntryHandler entryHandler = new EntryHandler(logger);

    private QuorumAckChecker quorumAckChecker = new QuorumAckChecker(logger);

    private Map<String, EntryDispatcher> dispatcherMap = new HashMap<>();

    public DLedgerEntryPusher(DLedgerConfig dLedgerConfig, MemberState memberState, DLedgerStore dLedgerStore,
        DLedgerRpcService dLedgerRpcService) {
        this.dLedgerConfig = dLedgerConfig;
        this.memberState = memberState;
        this.dLedgerStore = dLedgerStore;
        this.dLedgerRpcService = dLedgerRpcService;

        /**
         * 遍历memberState.getPeerMap()  将非SelfId的peer注入到dispatcherMap
         */
        for (String peer : memberState.getPeerMap().keySet()) {
            if (!peer.equals(memberState.getSelfId())) {
                dispatcherMap.put(peer, new EntryDispatcher(peer, logger));
            }
        }
    }

    public void startup() {
        /**
         * follower
         * Accept the push request and order it by the index, then append to ledger store one by one.
         */
        entryHandler.start();

        /**
         * leader
         * This thread will check the quorum index and complete the pending requests.
         */
        quorumAckChecker.start();

        /**
         * leader
         * This thread will push the entry to follower(identified by peerId) and update the completed pushed index to index map
         */
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

    /**
     * follower接受leader推送得消息
     * @param request
     * @return
     * @throws Exception
     */
    public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        return entryHandler.handlePush(request);
    }

    /**
     * 检查term对应的peerWaterMarksByTerm是否存在    不存在则创建
     * @param term
     * @param env
     */
    private void checkTermForWaterMark(long term, String env) {
        if (!peerWaterMarksByTerm.containsKey(term)) {
            logger.info("Initialize the watermark in {} for term={}", env, term);
            ConcurrentMap<String, Long> waterMarks = new ConcurrentHashMap<>();
            for (String peer : memberState.getPeerMap().keySet()) {
                waterMarks.put(peer, -1L);
            }
            peerWaterMarksByTerm.putIfAbsent(term, waterMarks);
        }
    }

    /**
     * 检查term对应的Pending是否存在   不存在则创建
     * @param term
     * @param env
     */
    private void checkTermForPendingMap(long term, String env) {
        if (!pendingAppendResponsesByTerm.containsKey(term)) {
            logger.info("Initialize the pending append map in {} for term={}", env, term);
            pendingAppendResponsesByTerm.putIfAbsent(term, new ConcurrentHashMap<>());
        }
    }

    /**
     * 更新peerWaterMarksByTerm   更新term&peerId对应的index
     * @param term
     * @param peerId
     * @param index
     */
    private void updatePeerWaterMark(long term, String peerId, long index) {
        synchronized (peerWaterMarksByTerm) {
            checkTermForWaterMark(term, "updatePeerWaterMark");
            if (peerWaterMarksByTerm.get(term).get(peerId) < index) {
                peerWaterMarksByTerm.get(term).put(peerId, index);
            }
        }
    }

    private long getPeerWaterMark(long term, String peerId) {
        synchronized (peerWaterMarksByTerm) {
            checkTermForWaterMark(term, "getPeerWaterMark");
            return peerWaterMarksByTerm.get(term).get(peerId);
        }
    }

    /**
     * 缓存是否已满  默认10000条数据
     * @param currTerm
     * @return
     */
    public boolean isPendingFull(long currTerm) {
        /**
         * 检查Pending是否存在  没有则创建
         */
        checkTermForPendingMap(currTerm, "isPendingFull");
        /**
         * 是否超过最大值
         */
        return pendingAppendResponsesByTerm.get(currTerm).size() > dLedgerConfig.getMaxPendingRequestsNum();
    }

    /**
     * 等待ack
     * @param entry
     * @return
     */
    public CompletableFuture<AppendEntryResponse> waitAck(DLedgerEntry entry) {
        /**
         * 更新peerWaterMarksByTerm
         */
        updatePeerWaterMark(entry.getTerm(), memberState.getSelfId(), entry.getIndex());
        /**
         * 集群中只有一个节点   直接返回
         */
        if (memberState.getPeerMap().size() == 1) {
            AppendEntryResponse response = new AppendEntryResponse();
            response.setGroup(memberState.getGroup());
            response.setLeaderId(memberState.getSelfId());
            response.setIndex(entry.getIndex());
            response.setTerm(entry.getTerm());
            response.setPos(entry.getPos());
            return AppendFuture.newCompletedFuture(entry.getPos(), response);
        } else {
            /**
             * 集群内有多个节点
             */
            checkTermForPendingMap(entry.getTerm(), "waitAck");
            AppendFuture<AppendEntryResponse> future = new AppendFuture<>(dLedgerConfig.getMaxWaitAckTimeMs());
            future.setPos(entry.getPos());
            /**
             * 将term pos index  注入到pendingAppendResponsesByTerm   等待QuorumAckChecker得调度执行
             */
            CompletableFuture<AppendEntryResponse> old = pendingAppendResponsesByTerm.get(entry.getTerm()).put(entry.getIndex(), future);
            if (old != null) {
                logger.warn("[MONITOR] get old wait at index={}", entry.getIndex());
            }
            /**
             * 唤醒Dispatchers   向follower发送push请求
             */
            wakeUpDispatchers();
            return future;
        }
    }

    public void wakeUpDispatchers() {
        for (EntryDispatcher dispatcher : dispatcherMap.values()) {
            dispatcher.wakeup();
        }
    }

    /**
     * This thread will check the quorum index and complete the pending requests.
     */
    private class QuorumAckChecker extends ShutdownAbleThread {

        private long lastPrintWatermarkTimeMs = System.currentTimeMillis();
        private long lastCheckLeakTimeMs = System.currentTimeMillis();
        private long lastQuorumIndex = -1;

        public QuorumAckChecker(Logger logger) {
            super("QuorumAckChecker", logger);
        }

        @Override
        public void doWork() {
            try {
                if (DLedgerUtils.elapsed(lastPrintWatermarkTimeMs) > 3000) {
                    logger.info("[{}][{}] term={} ledgerBegin={} ledgerEnd={} committed={} watermarks={}",
                        memberState.getSelfId(), memberState.getRole(), memberState.currTerm(), dLedgerStore.getLedgerBeginIndex(), dLedgerStore.getLedgerEndIndex(), dLedgerStore.getCommittedIndex(), JSON.toJSONString(peerWaterMarksByTerm));
                    lastPrintWatermarkTimeMs = System.currentTimeMillis();
                }
                if (!memberState.isLeader()) {
                    waitForRunning(1);
                    return;
                }

                /**
                 * 仅Leader时执行
                 */
                long currTerm = memberState.currTerm();
                checkTermForPendingMap(currTerm, "QuorumAckChecker");
                checkTermForWaterMark(currTerm, "QuorumAckChecker");
                if (pendingAppendResponsesByTerm.size() > 1) {
                    /**
                     * 清除并立即返回非currTerm的缓存数据   返回码为DLedgerResponseCode.TERM_CHANGED
                     */
                    for (Long term : pendingAppendResponsesByTerm.keySet()) {
                        if (term == currTerm) {
                            continue;
                        }
                        /**
                         * 立即返回TERM_CHANGED
                         */
                        for (Map.Entry<Long, TimeoutFuture<AppendEntryResponse>> futureEntry : pendingAppendResponsesByTerm.get(term).entrySet()) {
                            AppendEntryResponse response = new AppendEntryResponse();
                            response.setGroup(memberState.getGroup());
                            response.setIndex(futureEntry.getKey());
                            response.setCode(DLedgerResponseCode.TERM_CHANGED.getCode());
                            response.setLeaderId(memberState.getLeaderId());
                            logger.info("[TermChange] Will clear the pending response index={} for term changed from {} to {}", futureEntry.getKey(), term, currTerm);
                            futureEntry.getValue().complete(response);
                        }
                        /**
                         * 清除缓存
                         */
                        pendingAppendResponsesByTerm.remove(term);
                    }
                }
                /**
                 * 清除非currTerm的缓存数据
                 */
                if (peerWaterMarksByTerm.size() > 1) {
                    for (Long term : peerWaterMarksByTerm.keySet()) {
                        if (term == currTerm) {
                            continue;
                        }
                        logger.info("[TermChange] Will clear the watermarks for term changed from {} to {}", term, currTerm);
                        peerWaterMarksByTerm.remove(term);
                    }
                }
                Map<String, Long> peerWaterMarks = peerWaterMarksByTerm.get(currTerm);

                /**
                 * 综合leader和followers的回应   判断之前的append操作是否得到超过半数+1节点的成功回应
                 * 如果有则更新当前CommittedIndex
                 */
                long quorumIndex = -1;
                /**
                 * 循环集群内节点目前写入的index
                 */
                for (Long index : peerWaterMarks.values()) {
//                    System.out.println("index======"+index);
                    int num = 0;
                    for (Long another : peerWaterMarks.values()) {
//                        System.out.println("another======"+another);
                        if (another >= index) {
                            num++;
                        }
                    }
                    /**
                     * 是否获得超过半数+1节点的成功回应
                     */
                    if (memberState.isQuorum(num) && index > quorumIndex) {
//                        System.out.println(index + "," +quorumIndex);
                        quorumIndex = index;
                    }
                }

                /**
                 * 修改CommittedIndex
                 */
                dLedgerStore.updateCommittedIndex(currTerm, quorumIndex);

                /**
                 * 轮询待响应客户端的请求
                 */
                ConcurrentMap<Long, TimeoutFuture<AppendEntryResponse>> responses = pendingAppendResponsesByTerm.get(currTerm);
                boolean needCheck = false;
                int ackNum = 0;
                if (quorumIndex >= 0) {
                    for (Long i = quorumIndex; i >= 0; i--) {
                        try {
                            CompletableFuture<AppendEntryResponse> future = responses.remove(i);
                            if (future == null) {
                                needCheck = lastQuorumIndex != -1 && lastQuorumIndex != quorumIndex && i != lastQuorumIndex;
                                break;
                            } else if (!future.isDone()) {
                                /**
                                 * 给客户端成功响应
                                 */
                                AppendEntryResponse response = new AppendEntryResponse();
                                response.setGroup(memberState.getGroup());
                                response.setTerm(currTerm);
                                response.setIndex(i);
                                response.setLeaderId(memberState.getSelfId());
                                response.setPos(((AppendFuture) future).getPos());
                                future.complete(response);
                            }
                            ackNum++;
                        } catch (Throwable t) {
                            logger.error("Error in ack to index={} term={}", i, currTerm, t);
                        }
                    }
                }

                /**
                 * 没有给客户端响应  即小于quorumIndex的都已经得到响应
                 * 则检查大于quorumIndex  是否存在超时现象
                 * 直到遇到为null 或者不超时为止
                 */
                if (ackNum == 0) {
                    for (long i = quorumIndex + 1; i < Integer.MAX_VALUE; i++) {
                        TimeoutFuture<AppendEntryResponse> future = responses.get(i);
                        if (future == null) {
                            /**
                             * 为空退出
                             */
                            break;
                        } else if (future.isTimeOut()) {
                            /**
                             * 超时响应
                             */
                            AppendEntryResponse response = new AppendEntryResponse();
                            response.setGroup(memberState.getGroup());
                            response.setCode(DLedgerResponseCode.WAIT_QUORUM_ACK_TIMEOUT.getCode());
                            response.setTerm(currTerm);
                            response.setIndex(i);
                            response.setLeaderId(memberState.getSelfId());
                            future.complete(response);
                        } else {
                            /**
                             * 未超时  退出
                             */
                            break;
                        }
                    }
                    waitForRunning(1);
                }

                /**
                 * ？？？？？
                 */
                if (DLedgerUtils.elapsed(lastCheckLeakTimeMs) > 1000 || needCheck) {
                    updatePeerWaterMark(currTerm, memberState.getSelfId(), dLedgerStore.getLedgerEndIndex());
                    for (Map.Entry<Long, TimeoutFuture<AppendEntryResponse>> futureEntry : responses.entrySet()) {
                        if (futureEntry.getKey() < quorumIndex) {
                            AppendEntryResponse response = new AppendEntryResponse();
                            response.setGroup(memberState.getGroup());
                            response.setTerm(currTerm);
                            response.setIndex(futureEntry.getKey());
                            response.setLeaderId(memberState.getSelfId());
                            response.setPos(((AppendFuture) futureEntry.getValue()).getPos());
                            futureEntry.getValue().complete(response);
                            responses.remove(futureEntry.getKey());
                        }
                    }
                    lastCheckLeakTimeMs = System.currentTimeMillis();
                }
                lastQuorumIndex = quorumIndex;
            } catch (Throwable t) {
                DLedgerEntryPusher.logger.error("Error in {}", getName(), t);
                DLedgerUtils.sleep(100);
            }
        }
    }

    /**
     * This thread will be activated by the leader.
     * This thread will push the entry to follower(identified by peerId) and update the completed pushed index to index map.
     * Should generate a single thread for each peer.
     * The push has 4 types:
     *   APPEND : append the entries to the follower
     *   COMPARE : if the leader changes, the new leader should compare its entries to follower's
     *   TRUNCATE : if the leader finished comparing by an index, the leader will send a request to truncate the follower's ledger
     *   COMMIT: usually, the leader will attach the committed index with the APPEND request, but if the append requests are few and scattered,
     *           the leader will send a pure request to inform the follower of committed index.
     *
     *   The common transferring between these types are as following:
     *
     *   COMPARE ---- TRUNCATE ---- APPEND ---- COMMIT
     *   ^                             |
     *   |---<-----<------<-------<----|
     *
     */
    private class EntryDispatcher extends ShutdownAbleThread {

        private AtomicReference<PushEntryRequest.Type> type = new AtomicReference<>(PushEntryRequest.Type.COMPARE);
        private long lastPushCommitTimeMs = -1;
        private String peerId;
        private long compareIndex = -1;
        private long writeIndex = -1;
        private int maxPendingSize = 1000;
        private long term = -1;
        private String leaderId = null;
        private long lastCheckLeakTimeMs = System.currentTimeMillis();
        /**
         * leader向follower推送后   等待响应得缓存集合
         * ConcurrentMap<index, timestamp>
         */
        private ConcurrentMap<Long, Long> pendingMap = new ConcurrentHashMap<>();
        private Quota quota = new Quota(dLedgerConfig.getPeerPushQuota());

        public EntryDispatcher(String peerId, Logger logger) {
            super("EntryDispatcher-" + memberState.getSelfId() + "-" + peerId, logger);
            this.peerId = peerId;
        }

        /**
         * 检查状态
         * @return
         */
        private boolean checkAndFreshState() {
            /**
             * 非leader
             */
            if (!memberState.isLeader()) {
                return false;
            }

            /**
             * 保证term leaderId与memberState中的值相同   并设置type为COMPARE
             */
            if (term != memberState.currTerm() || leaderId == null || !leaderId.equals(memberState.getLeaderId())) {
                synchronized (memberState) {
                    if (!memberState.isLeader()) {
                        return false;
                    }
                    PreConditions.check(memberState.getSelfId().equals(memberState.getLeaderId()), DLedgerResponseCode.UNKNOWN);
                    term = memberState.currTerm();
                    leaderId = memberState.getSelfId();
                    /**
                     * 修改状态
                     */
                    changeState(-1, PushEntryRequest.Type.COMPARE);
                }
            }
            return true;
        }

        private PushEntryRequest buildPushRequest(DLedgerEntry entry, PushEntryRequest.Type target) {
            PushEntryRequest request = new PushEntryRequest();
            request.setGroup(memberState.getGroup());
            request.setRemoteId(peerId);
            request.setLeaderId(leaderId);
            request.setTerm(term);
            request.setEntry(entry);
            request.setType(target);
            /**
             * 每次向follower发送的push请求    都会将当前leader端的commitindex传递过去   让follower来同步
             */
            request.setCommitIndex(dLedgerStore.getCommittedIndex());
            return request;
        }

        /**
         *
         * @param entry
         */
        private void checkQuotaAndWait(DLedgerEntry entry) {
            if (dLedgerStore.getLedgerEndIndex() - entry.getIndex() <= maxPendingSize) {
                return;
            }
            if (dLedgerStore instanceof DLedgerMemoryStore) {
                return;
            }
            DLedgerMmapFileStore mmapFileStore = (DLedgerMmapFileStore) dLedgerStore;
            if (mmapFileStore.getDataFileList().getMaxWrotePosition() - entry.getPos() < dLedgerConfig.getPeerPushThrottlePoint()) {
                return;
            }
            quota.sample(entry.getSize());
            if (quota.validateNow()) {
                DLedgerUtils.sleep(quota.leftNow());
            }
        }

        /**
         * 向follower推送append
         * @param index
         * @throws Exception
         */
        private void doAppendInner(long index) throws Exception {
            /**
             * 获得index处data数据
             */
            DLedgerEntry entry = dLedgerStore.get(index);
            PreConditions.check(entry != null, DLedgerResponseCode.UNKNOWN, "writeIndex=%d", index);
            checkQuotaAndWait(entry);
            PushEntryRequest request = buildPushRequest(entry, PushEntryRequest.Type.APPEND);
            /**
             * 向follower推送append
             */
            CompletableFuture<PushEntryResponse> responseFuture = dLedgerRpcService.push(request);
            /**
             * 等待follower反馈得缓存
             */
            pendingMap.put(index, System.currentTimeMillis());
            responseFuture.whenComplete((x, ex) -> {
                try {
                    PreConditions.check(ex == null, DLedgerResponseCode.UNKNOWN);
                    DLedgerResponseCode responseCode = DLedgerResponseCode.valueOf(x.getCode());
                    switch (responseCode) {
                        case SUCCESS:
                            /**
                             * 移除缓存
                             */
                            pendingMap.remove(x.getIndex());
                            /**
                             * 更新peerWaterMarksByTerm
                             */
                            updatePeerWaterMark(x.getTerm(), peerId, x.getIndex());
                            /**
                             * 唤醒QuorumAckChecker
                             */
                            quorumAckChecker.wakeup();
                            break;
                        case INCONSISTENT_STATE:
                            logger.info("[Push-{}]Get INCONSISTENT_STATE when push index={} term={}", peerId, x.getIndex(), x.getTerm());
                            changeState(-1, PushEntryRequest.Type.COMPARE);
                            break;
                        default:
                            logger.warn("[Push-{}]Get error response code {} {}", peerId, responseCode, x.baseInfo());
                            break;
                    }
                } catch (Throwable t) {
                    logger.error("", t);
                }
            });
            lastPushCommitTimeMs = System.currentTimeMillis();
        }

        /**
         * leader向follower推送COMMIT
         * @throws Exception
         */
        private void doCommit() throws Exception {
            if (DLedgerUtils.elapsed(lastPushCommitTimeMs) > 1000) {
                PushEntryRequest request = buildPushRequest(null, PushEntryRequest.Type.COMMIT);
                //Ignore the results
                dLedgerRpcService.push(request);
                lastPushCommitTimeMs = System.currentTimeMillis();
            }
        }

        /**
         * 如果缓存中的数据超时  则马上执行doAppendInner操作
         * @throws Exception
         */
        private void doCheckAppendResponse() throws Exception {
            /**
             * 获取当前term   peerId存储的最大index
             */
            long peerWaterMark = getPeerWaterMark(term, peerId);
            /**
             * 获取缓存中peerWaterMark+1对应的消息的存储时间
             */
            Long sendTimeMs = pendingMap.get(peerWaterMark + 1);
            /**
             * 超时  则马上执行doAppendInner操作
             */
            if (sendTimeMs != null && System.currentTimeMillis() - sendTimeMs > dLedgerConfig.getMaxPushTimeOutMs()) {
                logger.warn("[Push-{}]Retry to push entry at {}", peerId, peerWaterMark + 1);
                doAppendInner(peerWaterMark + 1);
            }
        }

        /**
         * append操作
         * @throws Exception
         */
        private void doAppend() throws Exception {
            while (true) {
                if (!checkAndFreshState()) {
                    break;
                }
                if (type.get() != PushEntryRequest.Type.APPEND) {
                    break;
                }

                /**
                 *
                 */
                if (writeIndex > dLedgerStore.getLedgerEndIndex()) {
//                    System.out.println(writeIndex+","+dLedgerStore.getLedgerEndIndex());
                    /**
                     * 向follower发起commit请求
                     */
                    doCommit();
                    doCheckAppendResponse();
                    break;
                }
                /**
                 * 缓存过大时  删除缓存
                 */
                if (pendingMap.size() >= maxPendingSize || (DLedgerUtils.elapsed(lastCheckLeakTimeMs) > 1000)) {
                    long peerWaterMark = getPeerWaterMark(term, peerId);
                    for (Long index : pendingMap.keySet()) {
                        if (index < peerWaterMark) {
                            pendingMap.remove(index);
                        }
                    }
                    lastCheckLeakTimeMs = System.currentTimeMillis();
                }

                /**
                 * 缓存超过最大值   doAppendInner
                 */
                if (pendingMap.size() >= maxPendingSize) {
                    doCheckAppendResponse();
                    break;
                }
                /**
                 * 向follower推送append
                 */
                doAppendInner(writeIndex);
                writeIndex++;
            }
        }

        /**
         * TRUNCATE操作
         * @param truncateIndex
         * @throws Exception
         */
        private void doTruncate(long truncateIndex) throws Exception {
            PreConditions.check(type.get() == PushEntryRequest.Type.TRUNCATE, DLedgerResponseCode.UNKNOWN);
            /**
             * 获取truncateIndex处的data数据
             */
            DLedgerEntry truncateEntry = dLedgerStore.get(truncateIndex);
            PreConditions.check(truncateEntry != null, DLedgerResponseCode.UNKNOWN);
            logger.info("[Push-{}]Will push data to truncate truncateIndex={} pos={}", peerId, truncateIndex, truncateEntry.getPos());
            PushEntryRequest truncateRequest = buildPushRequest(truncateEntry, PushEntryRequest.Type.TRUNCATE);
            /**
             * 推送TRUNCATE请求
             */
            PushEntryResponse truncateResponse = dLedgerRpcService.push(truncateRequest).get(3, TimeUnit.SECONDS);
            PreConditions.check(truncateResponse != null, DLedgerResponseCode.UNKNOWN, "truncateIndex=%d", truncateIndex);
            PreConditions.check(truncateResponse.getCode() == DLedgerResponseCode.SUCCESS.getCode(), DLedgerResponseCode.valueOf(truncateResponse.getCode()), "truncateIndex=%d", truncateIndex);
            lastPushCommitTimeMs = System.currentTimeMillis();
            changeState(truncateIndex, PushEntryRequest.Type.APPEND);

            /**
             * TRUNCATE如果失败   是否是要重新进行COMPARE操作？？？？
             */
        }

        /**
         * 修改状态
         * @param index
         * @param target
         */
        private synchronized void changeState(long index, PushEntryRequest.Type target) {
            logger.info("[Push-{}]Change state from {} to {} at {}", peerId, type.get(), target, index);
            switch (target) {
                case APPEND:
                    compareIndex = -1;
                    updatePeerWaterMark(term, peerId, index);
                    quorumAckChecker.wakeup();
                    writeIndex = index + 1;
                    break;
                case COMPARE:
                    /**
                     * 当前状态为APPEND
                     * 则修改为COMPARE  并重置compareIndex、pendingMap
                     */
                    if (this.type.compareAndSet(PushEntryRequest.Type.APPEND, PushEntryRequest.Type.COMPARE)) {
                        compareIndex = -1;
                        pendingMap.clear();
                    }
                    break;
                case TRUNCATE:
                    compareIndex = -1;
                    break;
                default:
                    break;
            }
            type.set(target);
        }

        private void doCompare() throws Exception {
            while (true) {
                /**
                 * 检查状态
                 */
                if (!checkAndFreshState()) {
                    break;
                }

                /**
                 * 只能为COMPARE或者TRUNCATE
                 */
                if (type.get() != PushEntryRequest.Type.COMPARE
                    && type.get() != PushEntryRequest.Type.TRUNCATE) {
                    break;
                }
                if (compareIndex == -1 && dLedgerStore.getLedgerEndIndex() == -1) {
                    break;
                }
                //revise the compareIndex
                /**
                 * 将比较的index重新赋值为leader的最终index
                 */
                if (compareIndex == -1) {
                    compareIndex = dLedgerStore.getLedgerEndIndex();
                    logger.info("[Push-{}][DoCompare] compareIndex=-1 means start to compare", peerId);
                } else if (compareIndex > dLedgerStore.getLedgerEndIndex() || compareIndex < dLedgerStore.getLedgerBeginIndex()) {
                    logger.info("[Push-{}][DoCompare] compareIndex={} out of range {}-{}", peerId, compareIndex, dLedgerStore.getLedgerBeginIndex(), dLedgerStore.getLedgerEndIndex());
                    compareIndex = dLedgerStore.getLedgerEndIndex();
                }

                /**
                 * 查询compareIndex处对应得data数据
                 */
                DLedgerEntry entry = dLedgerStore.get(compareIndex);
                PreConditions.check(entry != null, DLedgerResponseCode.INTERNAL_ERROR, "compareIndex=%d", compareIndex);

                /**
                 * 构建请求对象   向其他follower推送消息
                 */
                PushEntryRequest request = buildPushRequest(entry, PushEntryRequest.Type.COMPARE);
                CompletableFuture<PushEntryResponse> responseFuture = dLedgerRpcService.push(request);

                /**
                 * 等待3秒
                 */
                PushEntryResponse response = responseFuture.get(3, TimeUnit.SECONDS);
                PreConditions.check(response != null, DLedgerResponseCode.INTERNAL_ERROR, "compareIndex=%d", compareIndex);
                PreConditions.check(response.getCode() == DLedgerResponseCode.INCONSISTENT_STATE.getCode() || response.getCode() == DLedgerResponseCode.SUCCESS.getCode()
                    , DLedgerResponseCode.valueOf(response.getCode()), "compareIndex=%d", compareIndex);
                long truncateIndex = -1;

                /**
                 * 处理结果
                 */
                if (response.getCode() == DLedgerResponseCode.SUCCESS.getCode()) {
                    /*
                     * The comparison is successful:
                     * 1.Just change to append state, if the follower's end index is equal the compared index.
                     * 2.Truncate the follower, if the follower has some dirty entries.
                     */
                    if (compareIndex == response.getEndIndex()) {
                        /**
                         * follower存储的最后一条消息等于当前leader的compareIndex处的数据
                         */
                        changeState(compareIndex, PushEntryRequest.Type.APPEND);
                        break;
                    } else {
                        truncateIndex = compareIndex;
                    }
                } else if (response.getEndIndex() < dLedgerStore.getLedgerBeginIndex()
                    || response.getBeginIndex() > dLedgerStore.getLedgerEndIndex()) {
                    /*
                     The follower's entries does not intersect with the leader.
                     This usually happened when the follower has crashed for a long time while the leader has deleted the expired entries.
                     Just truncate the follower.
                     */
                    /**
                     * 这通常发生在follower崩溃了很长一段时间，而领导者删除了过期的条目
                     */
                    truncateIndex = dLedgerStore.getLedgerBeginIndex();
                } else if (compareIndex < response.getBeginIndex()) {
                    /*
                     The compared index is smaller than the follower's begin index.
                     This happened rarely, usually means some disk damage.
                     Just truncate the follower.
                     */
                    /**
                     * 这种情况很少发生，通常意味着一些磁盘损坏。
                     */
                    truncateIndex = dLedgerStore.getLedgerBeginIndex();
                } else if (compareIndex > response.getEndIndex()) {
                    /*
                     The compared index is bigger than the follower's end index.
                     This happened frequently. For the compared index is usually starting from the end index of the leader.
                     */
                    /**
                     * 这种情况经常发生。因为compareIndex通常是从leader的最终index开始的。
                     */
                    compareIndex = response.getEndIndex();
                } else {
                    /*
                      Compare failed and the compared index is in the range of follower's entries.
                     */
                    /**
                     * 比较失败，compareIndex在follower index的范围内。
                     * 即  response.getBeginIndex() <= compareIndex <= response.getEndIndex()
                     */
                    compareIndex--;
                }
                /*
                 The compared index is smaller than the leader's begin index, truncate the follower.
                 */
                if (compareIndex < dLedgerStore.getLedgerBeginIndex()) {
                    truncateIndex = dLedgerStore.getLedgerBeginIndex();
                }
                /*
                 If get value for truncateIndex, do it right now.
                 */
                if (truncateIndex != -1) {
                    /**
                     * 执行TRUNCATE操作
                     */
                    changeState(truncateIndex, PushEntryRequest.Type.TRUNCATE);
                    doTruncate(truncateIndex);
                    break;
                }
            }
            //while结束
        }

        @Override
        public void doWork() {
            try {
                if (!checkAndFreshState()) {
                    waitForRunning(1);
                    return;
                }

                if (type.get() == PushEntryRequest.Type.APPEND) {
                    doAppend();
                } else {
                    doCompare();
                }
                waitForRunning(1);
            } catch (Throwable t) {
                DLedgerEntryPusher.logger.error("[Push-{}]Error in {} writeIndex={} compareIndex={}", peerId, getName(), writeIndex, compareIndex, t);
                DLedgerUtils.sleep(500);
            }
        }
    }

    /**
     * This thread will be activated by the follower.
     * Accept the push request and order it by the index, then append to ledger store one by one.
     *
     */
    private class EntryHandler extends ShutdownAbleThread {

        private long lastCheckFastForwardTimeMs = System.currentTimeMillis();
        /**ConcurrentMap<index, Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>>**/
        ConcurrentMap<Long, Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>> writeRequestMap = new ConcurrentHashMap<>();
        BlockingQueue<Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>> compareOrTruncateRequests = new ArrayBlockingQueue<Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>>(100);

        public EntryHandler(Logger logger) {
            super("EntryHandler", logger);
        }

        /**
         * follower接受leader推送得消息
         *
         * 将请求放入compareOrTruncateRequests中  等待EntryHandler处理
         * @param request
         * @return
         * @throws Exception
         */
        public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
            //The timeout should smaller than the remoting layer's request timeout
            CompletableFuture<PushEntryResponse> future = new TimeoutFuture<>(1000);
            switch (request.getType()) {
                case APPEND:
                    PreConditions.check(request.getEntry() != null, DLedgerResponseCode.UNEXPECTED_ARGUMENT);
                    long index = request.getEntry().getIndex();
                    /**
                     * 注入writeRequestMap  等待EntryHandler的doWork调用
                     */
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> old = writeRequestMap.putIfAbsent(index, new Pair<>(request, future));
                    if (old != null) {
                        logger.warn("[MONITOR]The index {} has already existed with {} and curr is {}", index, old.getKey().baseInfo(), request.baseInfo());
                        future.complete(buildResponse(request, DLedgerResponseCode.REPEATED_PUSH.getCode()));
                    }
                    break;
                case COMMIT:
                    /**
                     * 注入compareOrTruncateRequests  等待EntryHandler的doWork调用
                     */
                    compareOrTruncateRequests.put(new Pair<>(request, future));
                    break;
                case COMPARE:
                case TRUNCATE:
                    PreConditions.check(request.getEntry() != null, DLedgerResponseCode.UNEXPECTED_ARGUMENT);
                    writeRequestMap.clear();
                    /**
                     * 注入compareOrTruncateRequests  等待EntryHandler的doWork调用
                     */
                    compareOrTruncateRequests.put(new Pair<>(request, future));
                    break;
                default:
                    logger.error("[BUG]Unknown type {} from {}", request.getType(), request.baseInfo());
                    future.complete(buildResponse(request, DLedgerResponseCode.UNEXPECTED_ARGUMENT.getCode()));
                    break;
            }
            return future;
        }

        /**
         * push返回对象
         * @param request
         * @param code
         * @return
         */
        private PushEntryResponse buildResponse(PushEntryRequest request, int code) {
            PushEntryResponse response = new PushEntryResponse();
            response.setGroup(request.getGroup());
            response.setCode(code);
            response.setTerm(request.getTerm());
            if (request.getType() != PushEntryRequest.Type.COMMIT) {
                response.setIndex(request.getEntry().getIndex());
            }
            response.setBeginIndex(dLedgerStore.getLedgerBeginIndex());
            response.setEndIndex(dLedgerStore.getLedgerEndIndex());
            return response;
        }

        /**
         * follower处理append
         * @param writeIndex
         * @param request
         * @param future
         */
        private void handleDoAppend(long writeIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(writeIndex == request.getEntry().getIndex(), DLedgerResponseCode.INCONSISTENT_STATE);
                /**
                 * appendAsFollower
                 */
                DLedgerEntry entry = dLedgerStore.appendAsFollower(request.getEntry(), request.getTerm(), request.getLeaderId());
                PreConditions.check(entry.getIndex() == writeIndex, DLedgerResponseCode.INCONSISTENT_STATE);
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                /**
                 * 每次push    leader都会上送CommittedIndex   以供follower修改CommittedIndex
                 */
                dLedgerStore.updateCommittedIndex(request.getTerm(), request.getCommitIndex());
            } catch (Throwable t) {
                logger.error("[HandleDoWrite] writeIndex={}", writeIndex, t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
        }

        /**
         * COMPARE操作
         * @param compareIndex
         * @param request
         * @param future
         * @return
         */
        private CompletableFuture<PushEntryResponse> handleDoCompare(long compareIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(compareIndex == request.getEntry().getIndex(), DLedgerResponseCode.UNKNOWN);
                PreConditions.check(request.getType() == PushEntryRequest.Type.COMPARE, DLedgerResponseCode.UNKNOWN);
                /**
                 * follower本地获取compareIndex对应的消息
                 */
                DLedgerEntry local = dLedgerStore.get(compareIndex);
                /**
                 * 比较两者是否相等
                 */
                PreConditions.check(request.getEntry().equals(local), DLedgerResponseCode.INCONSISTENT_STATE);
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
            } catch (Throwable t) {
                logger.error("[HandleDoCompare] compareIndex={}", compareIndex, t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
            return future;
        }

        /**
         * COMMIT
         * @param committedIndex
         * @param request
         * @param future
         * @return
         */
        private CompletableFuture<PushEntryResponse> handleDoCommit(long committedIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(committedIndex == request.getCommitIndex(), DLedgerResponseCode.UNKNOWN);
                PreConditions.check(request.getType() == PushEntryRequest.Type.COMMIT, DLedgerResponseCode.UNKNOWN);
                /**
                 * leader上送CommittedIndex   以供follower修改CommittedIndex
                 */
                dLedgerStore.updateCommittedIndex(request.getTerm(), committedIndex);
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
            } catch (Throwable t) {
                logger.error("[HandleDoCommit] committedIndex={}", request.getCommitIndex(), t);
                future.complete(buildResponse(request, DLedgerResponseCode.UNKNOWN.getCode()));
            }
            return future;
        }

        /**
         * TRUNCATE
         * @param truncateIndex
         * @param request
         * @param future
         * @return
         */
        private CompletableFuture<PushEntryResponse> handleDoTruncate(long truncateIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                logger.info("[HandleDoTruncate] truncateIndex={} pos={}", truncateIndex, request.getEntry().getPos());
                PreConditions.check(truncateIndex == request.getEntry().getIndex(), DLedgerResponseCode.UNKNOWN);
                PreConditions.check(request.getType() == PushEntryRequest.Type.TRUNCATE, DLedgerResponseCode.UNKNOWN);
                /**
                 * TRUNCATE
                 */
                long index = dLedgerStore.truncate(request.getEntry(), request.getTerm(), request.getLeaderId());
                PreConditions.check(index == truncateIndex, DLedgerResponseCode.INCONSISTENT_STATE);
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                /**
                 * 每次push    leader都会上送CommittedIndex   以供follower修改CommittedIndex
                 */
                dLedgerStore.updateCommittedIndex(request.getTerm(), request.getCommitIndex());
            } catch (Throwable t) {
                logger.error("[HandleDoTruncate] truncateIndex={}", truncateIndex, t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
            return future;
        }

        /**
         * The leader does push entries to follower, and record the pushed index. But in the following conditions, the push may get stopped.
         *   * If the follower is abnormally shutdown, its ledger end index may be smaller than before. At this time, the leader may push fast-forward entries, and retry all the time.
         *   * If the last ack is missed, and no new message is coming in.The leader may retry push the last message, but the follower will ignore it.
         * @param endIndex
         */
        private void checkAbnormalFuture(long endIndex) {
            if (DLedgerUtils.elapsed(lastCheckFastForwardTimeMs) < 1000) {
                return;
            }
            lastCheckFastForwardTimeMs  = System.currentTimeMillis();
            if (writeRequestMap.isEmpty()) {
                return;
            }
            /**
             * writeRequestMap不为空
             * 即writeRequestMap中的值不连续   有些leader推送过来的对象没有存储到writeRequestMap
             */
            long minFastForwardIndex = Long.MAX_VALUE;
            for (Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair : writeRequestMap.values()) {
                long index = pair.getKey().getEntry().getIndex();
                //Fall behind
                /**
                 * 删除writeRequestMap中   小于等于endIndex的数据
                 */
                if (index <= endIndex) {
                    try {
                        DLedgerEntry local = dLedgerStore.get(index);
                        PreConditions.check(pair.getKey().getEntry().equals(local), DLedgerResponseCode.INCONSISTENT_STATE);
                        pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.SUCCESS.getCode()));
                        logger.warn("[PushFallBehind]The leader pushed an entry index={} smaller than current ledgerEndIndex={}, maybe the last ack is missed", index, endIndex);
                    } catch (Throwable t) {
                        logger.error("[PushFallBehind]The leader pushed an entry index={} smaller than current ledgerEndIndex={}, maybe the last ack is missed", index, endIndex, t);
                        pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
                    }
                    writeRequestMap.remove(index);
                    continue;
                }
                //Just OK
                /**
                 * 刚刚好有下一个entry到达   退出方法   等待下次dowork处理append
                 */
                if (index ==  endIndex + 1) {
                    //The next entry is coming, just return
                    return;
                }
                //Fast forward
                TimeoutFuture<PushEntryResponse> future  = (TimeoutFuture<PushEntryResponse>) pair.getValue();
                if (!future.isTimeOut()) {
                    continue;
                }
                /**
                 * future已经超时   需要返回给leader响应
                 *
                 * minFastForwardIndex的值为future已经超时且最小的一个   因为下个循环中的index值一定比当前的index值大
                 */
                if (index < minFastForwardIndex) {
                    minFastForwardIndex = index;
                }
            }
            //for 结束
            if (minFastForwardIndex == Long.MAX_VALUE) {
                return;
            }
            Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = writeRequestMap.get(minFastForwardIndex);
            if (pair == null) {
                return;
            }
            logger.warn("[PushFastForward] ledgerEndIndex={} entryIndex={}", endIndex, minFastForwardIndex);
            /**
             * 返回响应   错误码为INCONSISTENT_STATE
             */
            pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
        }

        @Override
        public void doWork() {
            try {
                /**
                 * 非FOLLOWER角色
                 */
                if (!memberState.isFollower()) {
                    waitForRunning(1);
                    return;
                }

                /**
                 * 处理TRUNCATE、COMPARE、COMMIT请求
                 */
                if (compareOrTruncateRequests.peek() != null) {
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = compareOrTruncateRequests.poll();
                    PreConditions.check(pair != null, DLedgerResponseCode.UNKNOWN);
                    switch (pair.getKey().getType()) {
                        case TRUNCATE:
                            handleDoTruncate(pair.getKey().getEntry().getIndex(), pair.getKey(), pair.getValue());
                            break;
                        case COMPARE:
                            handleDoCompare(pair.getKey().getEntry().getIndex(), pair.getKey(), pair.getValue());
                            break;
                        case COMMIT:
                            handleDoCommit(pair.getKey().getCommitIndex(), pair.getKey(), pair.getValue());
                            break;
                        default:
                            break;
                    }
                } else {
                    /**
                     * 处理APPEND
                     */
                    long nextIndex = dLedgerStore.getLedgerEndIndex() + 1;
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = writeRequestMap.remove(nextIndex);
                    /**
                     * writeRequestMap中没有index对应的value
                     */
                    if (pair == null) {
                        /**
                         * 处理异常的future
                         */
                        checkAbnormalFuture(dLedgerStore.getLedgerEndIndex());
                        waitForRunning(1);
                        return;
                    }
                    PushEntryRequest request = pair.getKey();
                    /**
                     * 处理append
                     */
                    handleDoAppend(nextIndex, request, pair.getValue());
                }
            } catch (Throwable t) {
                DLedgerEntryPusher.logger.error("Error in {}", getName(), t);
                DLedgerUtils.sleep(100);
            }
        }
    }
}
