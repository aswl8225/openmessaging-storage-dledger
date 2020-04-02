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

public class DLedgerLeaderElector {

    private static Logger logger = LoggerFactory.getLogger(DLedgerLeaderElector.class);

    private Random random = new Random();
    private DLedgerConfig dLedgerConfig;
    private final MemberState memberState;
    private DLedgerRpcService dLedgerRpcService;

    //as a server handler
    //record the last leader state
    //follower上次接受leader心跳的时间
    private long lastLeaderHeartBeatTime = -1;
    //leader上次向follower发送心跳的时间
    private long lastSendHeartBeatTime = -1;
    //leader上次获得超过半数follower回应心跳的时间
    private long lastSuccHeartBeatTime = -1;
    private int heartBeatTimeIntervalMs = 2000;
    private int maxHeartBeatLeak = 3;
    //as a client
    private long nextTimeToRequestVote = -1;
    private boolean needIncreaseTermImmediately = false;
    private int minVoteIntervalMs = 300;
    private int maxVoteIntervalMs = 1000;

    private List<RoleChangeHandler> roleChangeHandlers = new ArrayList<>();

    private VoteResponse.ParseResult lastParseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
    private long lastVoteCost = 0L;

    private StateMaintainer stateMaintainer = new StateMaintainer("StateMaintainer", logger);

    private final TakeLeadershipTask takeLeadershipTask = new TakeLeadershipTask();

    /**
     * 选举leader
     * @param dLedgerConfig
     * @param memberState
     * @param dLedgerRpcService
     */
    public DLedgerLeaderElector(DLedgerConfig dLedgerConfig, MemberState memberState,
        DLedgerRpcService dLedgerRpcService) {
        this.dLedgerConfig = dLedgerConfig;
        this.memberState = memberState;
        this.dLedgerRpcService = dLedgerRpcService;

        /**
         * 设置时间间隔
         */
        refreshIntervals(dLedgerConfig);
    }

    public void startup() {
        /**
         * 根据自身角色
         */
        stateMaintainer.start();

        /**
         * 交由业务系统负责  例如rocketmq
         */
        for (RoleChangeHandler roleChangeHandler : roleChangeHandlers) {
            roleChangeHandler.startup();
        }
    }

    public void shutdown() {
        stateMaintainer.shutdown();
        for (RoleChangeHandler roleChangeHandler : roleChangeHandlers) {
            roleChangeHandler.shutdown();
        }
    }

    /**
     * 设置时间间隔
     * @param dLedgerConfig
     */
    private void refreshIntervals(DLedgerConfig dLedgerConfig) {
        //2000
        this.heartBeatTimeIntervalMs = dLedgerConfig.getHeartBeatTimeIntervalMs();
        //3
        this.maxHeartBeatLeak = dLedgerConfig.getMaxHeartBeatLeak();
        //300ms
        this.minVoteIntervalMs = dLedgerConfig.getMinVoteIntervalMs();
        //1000ms
        this.maxVoteIntervalMs = dLedgerConfig.getMaxVoteIntervalMs();
    }

    /**
     * 接受leader的心跳
     * @param request
     * @return
     * @throws Exception
     */
    public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception {
        /**
         * 请求的LeaderId与节点属于一个集群
         */
        if (!memberState.isPeerMember(request.getLeaderId())) {
            logger.warn("[BUG] [HandleHeartBeat] remoteId={} is an unknown member", request.getLeaderId());
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.UNKNOWN_MEMBER.getCode()));
        }

        /**
         * follow被认为是leader
         */
        if (memberState.getSelfId().equals(request.getLeaderId())) {
            logger.warn("[BUG] [HandleHeartBeat] selfId={} but remoteId={}", memberState.getSelfId(), request.getLeaderId());
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.UNEXPECTED_MEMBER.getCode()));
        }

        /**
         * 请求的选期小于当前节点的选期
         */
        if (request.getTerm() < memberState.currTerm()) {
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.EXPIRED_TERM.getCode()));
        } else if (request.getTerm() == memberState.currTerm()) {
            if (request.getLeaderId().equals(memberState.getLeaderId())) {
                /**
                 * term相同且有共同的leader  则更新上次心跳时间  并返回HeartBeatResponse   success
                 */
                lastLeaderHeartBeatTime = System.currentTimeMillis();
                return CompletableFuture.completedFuture(new HeartBeatResponse());
            }
        }

        //abnormal case
        //hold the lock to get the latest term and leaderId
        synchronized (memberState) {
            if (request.getTerm() < memberState.currTerm()) {
                /**
                 * 请求的选期小于当前选期
                 */
                return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.EXPIRED_TERM.getCode()));
            } else if (request.getTerm() == memberState.currTerm()) {
                /**
                 * 选期一致
                 */
                if (memberState.getLeaderId() == null) {
                    /**
                     * 当前节点没有选出leader  则自动降级为follow
                     */
                    changeRoleToFollower(request.getTerm(), request.getLeaderId());
                    return CompletableFuture.completedFuture(new HeartBeatResponse());
                } else if (request.getLeaderId().equals(memberState.getLeaderId())) {
                    /**
                     * leader相同
                     */
                    lastLeaderHeartBeatTime = System.currentTimeMillis();
                    return CompletableFuture.completedFuture(new HeartBeatResponse());
                } else {
                    /**
                     * 选期一致  但是leader不同   通常不会发生  但防止意外
                     */
                    //this should not happen, but if happened
                    logger.error("[{}][BUG] currTerm {} has leader {}, but received leader {}", memberState.getSelfId(), memberState.currTerm(), memberState.getLeaderId(), request.getLeaderId());
                    return CompletableFuture.completedFuture(new HeartBeatResponse().code(DLedgerResponseCode.INCONSISTENT_LEADER.getCode()));
                }
            } else {
                /**
                 * 请求term大于当前term
                 *
                 * 简而言之  对于具体差异的term   不建议马上将当前节点转换为follower
                 * 而是首先将其转换为candidate  并通知state-maintainer线程
                 */
                //To make it simple, for larger term, do not change to follower immediately
                //first change to candidate, and notify the state-maintainer thread
                changeRoleToCandidate(request.getTerm());
                needIncreaseTermImmediately = true;
                //TOOD notify
                return CompletableFuture.completedFuture(new HeartBeatResponse().code(DLedgerResponseCode.TERM_NOT_READY.getCode()));
            }
        }
    }

    /**
     * 晋升为leader
     * @param term
     */
    public void changeRoleToLeader(long term) {
        synchronized (memberState) {
            if (memberState.currTerm() == term) {

                /**
                 * 晋升为leader
                 */
                memberState.changeToLeader(term);
                lastSendHeartBeatTime = -1;

                /**
                 * 通知rocketmq
                 */
                handleRoleChange(term, MemberState.Role.LEADER);
                logger.info("[{}] [ChangeRoleToLeader] from term: {} and currTerm: {}", memberState.getSelfId(), term, memberState.currTerm());
            } else {
                logger.warn("[{}] skip to be the leader in term: {}, but currTerm is: {}", memberState.getSelfId(), term, memberState.currTerm());
            }
        }
    }

    /**
     * 修改节点角色为Candidate
     * @param term
     */
    public void changeRoleToCandidate(long term) {
        synchronized (memberState) {
            /**
             * 其他节点请求中的选期大于当前选期
             */
            if (term >= memberState.currTerm()) {
                /**
                 * 修改节点角色为Candidate
                 */
                memberState.changeToCandidate(term);

                /**
                 * 通知
                 */
                handleRoleChange(term, MemberState.Role.CANDIDATE);
                logger.info("[{}] [ChangeRoleToCandidate] from term: {} and currTerm: {}", memberState.getSelfId(), term, memberState.currTerm());
            } else {
                logger.info("[{}] skip to be candidate in term: {}, but currTerm: {}", memberState.getSelfId(), term, memberState.currTerm());
            }
        }
    }

    //just for test
    public void testRevote(long term) {
        changeRoleToCandidate(term);
        lastParseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
        nextTimeToRequestVote = -1;
    }

    /**
     * 变更为Follower
     * @param term
     * @param leaderId
     */
    public void changeRoleToFollower(long term, String leaderId) {
        logger.info("[{}][ChangeRoleToFollower] from term: {} leaderId: {} and currTerm: {}", memberState.getSelfId(), term, leaderId, memberState.currTerm());
        lastParseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
        /**
         * 变更为Follower
         */
        memberState.changeToFollower(term, leaderId);
        lastLeaderHeartBeatTime = System.currentTimeMillis();
        /**
         * 通知
         */
        handleRoleChange(term, MemberState.Role.FOLLOWER);
    }

    /**
     * 处理集群内节点的选举请求  包括本身
     * @param request
     * @param self  当前请求是否来自于节点本身
     * @return
     */
    public CompletableFuture<VoteResponse> handleVote(VoteRequest request, boolean self) {
        //hold the lock to get the latest term, leaderId, ledgerEndIndex
        synchronized (memberState) {
            /**
             * 请求的LeaderId与节点属于一个集群
             */
            if (!memberState.isPeerMember(request.getLeaderId())) {
                logger.warn("[BUG] [HandleVote] remoteId={} is an unknown member", request.getLeaderId());
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_UNKNOWN_LEADER));
            }

            /**
             * 远端节点发送请求   且选举的leader为当前节点
             * 因为节点在选举的时候  都是默认选举本身节点   所以不应该发生选举其他节点为leader的情况
             */
            if (!self && memberState.getSelfId().equals(request.getLeaderId())) {
                logger.warn("[BUG] [HandleVote] selfId={} but remoteId={}", memberState.getSelfId(), request.getLeaderId());
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_UNEXPECTED_LEADER));
            }

            /**
             * 请求中的选期小于当前的选期
             */
            if (request.getTerm() < memberState.currTerm()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_EXPIRED_VOTE_TERM));
            } else if (request.getTerm() == memberState.currTerm()) {
                /**
                 * 请求中的选期等于当前的选期
                 */
                if (memberState.currVoteFor() == null) {
                    //let it go
                    /**
                     * 节点目前没有投票对象
                     */
                } else if (memberState.currVoteFor().equals(request.getLeaderId())) {
                    //repeat just let it go
                    /**
                     * 节点投票的对象就是请求中的LeaderId
                     */
                } else {
                    /**
                     * 节点投票的对象不是请求中的LeaderId
                     */

                    /**
                     * 当前节点已经有对应的leader
                     */
                    if (memberState.getLeaderId() != null) {
                        return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_ALREADY_HAS_LEADER));
                    } else {
                        /**
                         * 当前节点没有对应的leader  但是已经投票给了其他节点
                         */
                        return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_ALREADY_VOTED));
                    }
                }
            } else {
                /**
                 * 请求中的选期大于当前的选期
                 */
                //stepped down by larger term

                /**
                 * 修改节点角色为Candidate
                 */
                changeRoleToCandidate(request.getTerm());
                needIncreaseTermImmediately = true;
                //only can handleVote when the term is consistent
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_TERM_NOT_READY));
            }

            //assert acceptedTerm is true
            if (request.getLedgerEndTerm() < memberState.getLedgerEndTerm()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_EXPIRED_LEDGER_TERM));
            } else if (request.getLedgerEndTerm() == memberState.getLedgerEndTerm() && request.getLedgerEndIndex() < memberState.getLedgerEndIndex()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_SMALL_LEDGER_END_INDEX));
            }

            if (request.getTerm() < memberState.getLedgerEndTerm()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.getLedgerEndTerm()).voteResult(VoteResponse.RESULT.REJECT_TERM_SMALL_THAN_LEDGER));
            }

            /**
             * 远端节点发送请求
             * 当前节点为优选节点||当前选期是优选节点的选期
             * request.getLedgerEndTerm() == memberState.getLedgerEndTerm()
             * memberState.getLedgerEndIndex() >= request.getLedgerEndIndex()
             *
             * 即远端节点发送的请求   且本地满足优选leader  且ledgerEndTerm满足条件   但是远程节点ledgerEndIndex小于本地ledgerEndIndex   则拒绝抢主
             */
            if (!self && isTakingLeadership() && request.getLedgerEndTerm() == memberState.getLedgerEndTerm() && memberState.getLedgerEndIndex() >= request.getLedgerEndIndex()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_TAKING_LEADERSHIP));
            }

            /**
             * 节点选举leader
             *
             * 远程节点选举的leader要和本地节点属于同一个集群
             * 远程节点选举的leader不能为当前节点
             * 远程节点term要 == 当前节点term
             * 当前节点没有投票对象或者当前节点就是投票给远端节点
             * 远程节点ledgerEndTerm == 本地节点ledgerEndTerm时，远端节点的ledgerEndIndex >= 本地ledgerEndIndex
             * 远程节点ledgerEndTerm > 本地节点ledgerEndTerm
             * 远程节点term >= 本地远程节点ledgerEndTerm
             * 远程节点发送的请求   且本地满足优选leader  且term额满足条件   本地ledgerEndIndex要 < 远程节点ledgerEndIndex
             *
             * 满足以上条件   本地节点才会同意远程节点的选举
             */
            memberState.setCurrVoteFor(request.getLeaderId());
            return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.ACCEPT));
        }
    }

    /**
     * 发送心跳
     * @param term
     * @param leaderId
     * @throws Exception
     */
    private void sendHeartbeats(long term, String leaderId) throws Exception {
        //所有回应的节点
        final AtomicInteger allNum = new AtomicInteger(1);
        //反馈成功的节点
        final AtomicInteger succNum = new AtomicInteger(1);
        //选期大于当前节点选期的节点数
        final AtomicInteger notReadyNum = new AtomicInteger(0);
        //集群中最大的选期
        final AtomicLong maxTerm = new AtomicLong(-1);
        //多个leader
        final AtomicBoolean inconsistLeader = new AtomicBoolean(false);
        //
        final CountDownLatch beatLatch = new CountDownLatch(1);
        long startHeartbeatTimeMs = System.currentTimeMillis();
        /**
         * 遍历集群内的节点  发送心跳
         */
        for (String id : memberState.getPeerMap().keySet()) {
            /**
             * 排除当前节点
             */
            if (memberState.getSelfId().equals(id)) {
                continue;
            }

            /**
             * 心跳请求
             */
            HeartBeatRequest heartBeatRequest = new HeartBeatRequest();
            heartBeatRequest.setGroup(memberState.getGroup());
            heartBeatRequest.setLocalId(memberState.getSelfId());
            heartBeatRequest.setRemoteId(id);
            heartBeatRequest.setLeaderId(leaderId);
            heartBeatRequest.setTerm(term);
            /**
             * 发送心跳
             */
            CompletableFuture<HeartBeatResponse> future = dLedgerRpcService.heartBeat(heartBeatRequest);
            future.whenComplete((HeartBeatResponse x, Throwable ex) -> {
                try {
                    if (ex != null) {
                        memberState.getPeersLiveTable().put(id, Boolean.FALSE);
                        throw ex;
                    }
                    switch (DLedgerResponseCode.valueOf(x.getCode())) {
                        case SUCCESS:
                            /**
                             * 成功
                             */
                            succNum.incrementAndGet();
                            break;
                        case EXPIRED_TERM:
                            /**
                             * 当前选期小于被请求节点的选期
                             */
                            maxTerm.set(x.getTerm());
                            break;
                        case INCONSISTENT_LEADER:
                            /**
                             * 同一个选期内有两个leader  通常不会发生  但防止意外
                             */
                            inconsistLeader.compareAndSet(false, true);
                            break;
                        case TERM_NOT_READY:
                            /**
                             * 其他节点的选期落后当前节点选期
                             */
                            notReadyNum.incrementAndGet();
                            break;
                        default:
                            break;
                    }

                    if (x.getCode() == DLedgerResponseCode.NETWORK_ERROR.getCode())
                        memberState.getPeersLiveTable().put(id, Boolean.FALSE);
                    else
                        memberState.getPeersLiveTable().put(id, Boolean.TRUE);

                    /**
                     * 同意的节点达到合法数量    同意的节点+选期小于当前节点选期的节点数达到合法数量
                     */
                    if (memberState.isQuorum(succNum.get())
                        || memberState.isQuorum(succNum.get() + notReadyNum.get())) {
                        /**
                         * 之后不需要在await
                         */
                        beatLatch.countDown();
                    }
                } catch (Throwable t) {
                    logger.error("heartbeat response failed", t);
                } finally {
                    allNum.incrementAndGet();
                    /**
                     * 全部节点都有反馈
                     */
                    if (allNum.get() == memberState.peerSize()) {
                        /**
                         * 之后不需要在await
                         */
                        beatLatch.countDown();
                    }
                }
            });
        }
        //for结束

        /**
         *
         */
        beatLatch.await(heartBeatTimeIntervalMs, TimeUnit.MILLISECONDS);
        if (memberState.isQuorum(succNum.get())) {
            /**
             * 同意的节点达到合法数量
             */
            lastSuccHeartBeatTime = System.currentTimeMillis();
        } else {
            logger.info("[{}] Parse heartbeat responses in cost={} term={} allNum={} succNum={} notReadyNum={} inconsistLeader={} maxTerm={} peerSize={} lastSuccHeartBeatTime={}",
                memberState.getSelfId(), DLedgerUtils.elapsed(startHeartbeatTimeMs), term, allNum.get(), succNum.get(), notReadyNum.get(), inconsistLeader.get(), maxTerm.get(), memberState.peerSize(), new Timestamp(lastSuccHeartBeatTime));
            if (memberState.isQuorum(succNum.get() + notReadyNum.get())) {
                /**
                 * 同意的节点+选期大于当前节点选期的节点数达到合法数量   下次直接发起heartbeat  无需等待
                 */
                lastSendHeartBeatTime = -1;
            } else if (maxTerm.get() > term) {
                /**
                 * 有其他节点的选期大于当前选期  则当前节点变更为Candidate
                 */
                changeRoleToCandidate(maxTerm.get());
            } else if (inconsistLeader.get()) {
                /**
                 * 集群中多个leader  则当前节点变更为Candidate
                 */
                changeRoleToCandidate(term);
            } else if (DLedgerUtils.elapsed(lastSuccHeartBeatTime) > maxHeartBeatLeak * heartBeatTimeIntervalMs) {
                /**
                 * 当前时间与上次成功收到follower反馈心跳的时间大于maxHeartBeatLeak * heartBeatTimeIntervalMs
                 * 则当前节点变更为Candidate
                 */
                changeRoleToCandidate(term);
            }
        }
    }

    /**
     * 向FOLLOWER发送心跳信号，当法定人数追随者不响应时，退到CANDIDATE。
     * @throws Exception
     */
    private void maintainAsLeader() throws Exception {
        /**
         * 两次心跳间隔2000ms
         */
        if (DLedgerUtils.elapsed(lastSendHeartBeatTime) > heartBeatTimeIntervalMs) {
            long term;
            String leaderId;
            synchronized (memberState) {
                if (!memberState.isLeader()) {
                    //stop sending
                    return;
                }
                term = memberState.currTerm();
                leaderId = memberState.getLeaderId();

                /**
                 * 发送心跳前   修改上次发送心跳时间
                 */
                lastSendHeartBeatTime = System.currentTimeMillis();
            }

            /**
             * 发送心跳
             */
            sendHeartbeats(term, leaderId);
        }
    }

    /**
     * 接受心跳，当LEADER没有心跳时，改为CANDIDATE。
     * @throws Exception
     */
    private void maintainAsFollower() {
        /**
         * 距离上次接受leader心跳的时间间隔大于两个周期
         */
        if (DLedgerUtils.elapsed(lastLeaderHeartBeatTime) > 2 * heartBeatTimeIntervalMs) {
            synchronized (memberState) {
                /**
                 * 当前节点为follower   且距离上次接受leader心跳的时间间隔大于maxHeartBeatLeak个周期
                 * follower变更为Candidate
                 */
                if (memberState.isFollower() && (DLedgerUtils.elapsed(lastLeaderHeartBeatTime) > maxHeartBeatLeak * heartBeatTimeIntervalMs)) {
                    logger.info("[{}][HeartBeatTimeOut] lastLeaderHeartBeatTime: {} heartBeatTimeIntervalMs: {} lastLeader={}", memberState.getSelfId(), new Timestamp(lastLeaderHeartBeatTime), heartBeatTimeIntervalMs, memberState.getLeaderId());
                    changeRoleToCandidate(memberState.currTerm());
                }
            }
        }
    }

    /**
     * 在集群内发起选举
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
            /**
             * 选举请求对象
             */
            VoteRequest voteRequest = new VoteRequest();
            voteRequest.setGroup(memberState.getGroup());
            voteRequest.setLedgerEndIndex(ledgerEndIndex);
            voteRequest.setLedgerEndTerm(ledgerEndTerm);

            /**
             * 选举节点本身
             */
            voteRequest.setLeaderId(memberState.getSelfId());
            voteRequest.setTerm(term);
            voteRequest.setRemoteId(id);
            CompletableFuture<VoteResponse> voteResponse;

            if (memberState.getSelfId().equals(id)) {
                /**
                 * 选举自己
                 */
                voteResponse = handleVote(voteRequest, true);
            } else {
                //async
                /**
                 * 向集群内其他节点发起选举通讯
                 */
                voteResponse = dLedgerRpcService.vote(voteRequest);
            }
            responses.add(voteResponse);

        }
        return responses;
    }

    /**
     * 当前节点是优选节点   或者  当前选期是优选节点的选期
     * @return
     */
    private boolean isTakingLeadership() {
        return memberState.getSelfId().equals(dLedgerConfig.getPreferredLeaderId())
            || memberState.getTermToTakeLeadership() == memberState.currTerm();
    }

    /**
     * 下次发起投票时间
     *
     * @return
     */
    private long getNextTimeToRequestVote() {
        /**
         * 当前节点是优选节点   或者  当前选期是优选节点的选期
         */
        if (isTakingLeadership()) {
            /**
             * 当前时间+（minTakeLeadershipVoteIntervalMs和maxTakeLeadershipVoteIntervalMs）之间的随机数
             */
            return System.currentTimeMillis() + dLedgerConfig.getMinTakeLeadershipVoteIntervalMs() +
                random.nextInt(dLedgerConfig.getMaxTakeLeadershipVoteIntervalMs() - dLedgerConfig.getMinTakeLeadershipVoteIntervalMs());
        }
        /**
         * 当前时间 + 上次投票消耗时间 + （minVoteIntervalMs和maxVoteIntervalMs）之间的随机数
         */
        return System.currentTimeMillis() + lastVoteCost + minVoteIntervalMs + random.nextInt(maxVoteIntervalMs - minVoteIntervalMs);
    }

    /**
     * 投票选举
     * @throws Exception
     */
    private void maintainAsCandidate() throws Exception {
        //for candidate
        /**
         * 没到选举时间
         */
        if (System.currentTimeMillis() < nextTimeToRequestVote && !needIncreaseTermImmediately) {
            return;
        }
        long term;
        long ledgerEndTerm;
        long ledgerEndIndex;
        synchronized (memberState) {
            /**
             * 非CANDIDATE
             */
            if (!memberState.isCandidate()) {
                return;
            }

            /**
             * 上一次投票结果为WAIT_TO_VOTE_NEXT时
             */
            if (lastParseResult == VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT || needIncreaseTermImmediately) {
                long prevTerm = memberState.currTerm();

                /**
                 * 更改选期并持久化到currterm文件
                 */
                term = memberState.nextTerm();
                logger.info("{}_[INCREASE_TERM] from {} to {}", memberState.getSelfId(), prevTerm, term);
                lastParseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            } else {
                /**
                 * 其他状态
                 */
                term = memberState.currTerm();
            }
            ledgerEndIndex = memberState.getLedgerEndIndex();
            ledgerEndTerm = memberState.getLedgerEndTerm();
        }

        /**
         * 立刻计算下次发起选举时间
         */
        if (needIncreaseTermImmediately) {
            /**
             * 下次投票时间
             */
            nextTimeToRequestVote = getNextTimeToRequestVote();
            needIncreaseTermImmediately = false;
            return;
        }

        long startVoteTimeMs = System.currentTimeMillis();

        /**
         * 发起选举
         */
        final List<CompletableFuture<VoteResponse>> quorumVoteResponses = voteForQuorumResponses(term, ledgerEndTerm, ledgerEndIndex);
        //集群中最大的Term
        final AtomicLong knownMaxTermInGroup = new AtomicLong(-1);
        //每次选举  有反馈的节点数
        final AtomicInteger allNum = new AtomicInteger(0);
        //合法的票数
        final AtomicInteger validNum = new AtomicInteger(0);
        //赞成的节点数
        final AtomicInteger acceptedNum = new AtomicInteger(0);
        //选举term比当前节点term小的节点数
        final AtomicInteger notReadyTermNum = new AtomicInteger(0);
        //LedgerEndTerm大于当前节点的LedgerEndTerm的节点数
        final AtomicInteger biggerLedgerNum = new AtomicInteger(0);
        //集群中已经有leader
        final AtomicBoolean alreadyHasLeader = new AtomicBoolean(false);

        CountDownLatch voteLatch = new CountDownLatch(1);
        /**
         * 遍历集群内节点对选举的反馈
         */
        for (CompletableFuture<VoteResponse> future : quorumVoteResponses) {
            future.whenComplete((VoteResponse x, Throwable ex) -> {
                try {
                    if (ex != null) {
                        throw ex;
                    }
                    logger.info("[{}][GetVoteResponse] {}", memberState.getSelfId(), JSON.toJSONString(x));
                    if (x.getVoteResult() != VoteResponse.RESULT.UNKNOWN) {
                        validNum.incrementAndGet();
                    }
                    synchronized (knownMaxTermInGroup) {
                        switch (x.getVoteResult()) {
                            case ACCEPT:
                                /**
                                 * 同意的选票
                                 */
                                acceptedNum.incrementAndGet();
                                break;
                            case REJECT_ALREADY_VOTED:
                            case REJECT_TAKING_LEADERSHIP:
                                break;
                            case REJECT_ALREADY_HAS_LEADER:
                                /**
                                 * 其他节点已经有leader  且选期一致   则设置alreadyHasLeader为true  即集群中已经有leader
                                 */
                                alreadyHasLeader.compareAndSet(false, true);
                                break;
                            case REJECT_TERM_SMALL_THAN_LEDGER:
                            case REJECT_EXPIRED_VOTE_TERM:
                                /**
                                 * 选期落后其他节点的选期   则修改knownMaxTermInGroup
                                 */
                                if (x.getTerm() > knownMaxTermInGroup.get()) {
                                    knownMaxTermInGroup.set(x.getTerm());
                                }
                                break;
                            case REJECT_EXPIRED_LEDGER_TERM:
                            case REJECT_SMALL_LEDGER_END_INDEX:
                                /**
                                 * 1、request.getTerm() = memberState.currTerm()
                                 * 2、memberState.currVoteFor()为null或者选举的leader就是request.getLeaderId()
                                 * 3、request.getLedgerEndTerm() = memberState.getLedgerEndTerm()
                                 * 4、request.getLedgerEndIndex() > memberState.getLedgerEndIndex()
                                 *
                                 * 即其他节点的LedgerEndIndex  大于当前节点的LedgerEndIndex
                                 */
                                biggerLedgerNum.incrementAndGet();
                                break;
                            case REJECT_TERM_NOT_READY:
                                /**
                                 * 其他节点的选期小于当前节点的选期   其他节点角色变更为Candidate
                                 */
                                notReadyTermNum.incrementAndGet();
                                break;
                            default:
                                break;

                        }
                    }

                    /**
                     * 其他节点已经有leader且选期一致    同意的节点达到合法数量    同意的节点+其他节点的选期小于当前节点的选期的节点数量达到合法数量
                     */
                    if (alreadyHasLeader.get()
                        || memberState.isQuorum(acceptedNum.get())
                        || memberState.isQuorum(acceptedNum.get() + notReadyTermNum.get())) {
                        voteLatch.countDown();
                    }
                } catch (Throwable t) {
                    logger.error("vote response failed", t);
                } finally {
                    allNum.incrementAndGet();
                    /**
                     * 所有节点都有回应
                     */
                    if (allNum.get() == memberState.peerSize()) {
                        voteLatch.countDown();
                    }
                }
            });

        }
        //for结束
        try {
            /**
             * 如果上面执行了voteLatch.countDown()   这里就直接结束
             *
             * 否则要等待3000 + random.nextInt(maxVoteIntervalMs)秒
             */
            voteLatch.await(3000 + random.nextInt(maxVoteIntervalMs), TimeUnit.MILLISECONDS);
        } catch (Throwable ignore) {

        }

        /**
         * 选举时间耗时
         */
        lastVoteCost = DLedgerUtils.elapsed(startVoteTimeMs);
        VoteResponse.ParseResult parseResult;
        if (knownMaxTermInGroup.get() > term) {
            /**
             * 集群内最大term大于当前term
             */
            parseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
            nextTimeToRequestVote = getNextTimeToRequestVote();
            /**
             * 更改角色为CANDIDATE
             */
            changeRoleToCandidate(knownMaxTermInGroup.get());
        } else if (alreadyHasLeader.get()) {
            /**
             * 集群内已经有leader   nextTimeToRequestVote相应延长3个心跳时间   似乎是在等待leader发送心跳
             */
            parseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            nextTimeToRequestVote = getNextTimeToRequestVote() + heartBeatTimeIntervalMs * maxHeartBeatLeak;
        } else if (!memberState.isQuorum(validNum.get())) {
            /**
             * 没有足够多的节点回应
             */
            parseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            nextTimeToRequestVote = getNextTimeToRequestVote();
        } else if (memberState.isQuorum(acceptedNum.get())) {
            /**
             * 节点内同意席数达到法定席数
             */
            parseResult = VoteResponse.ParseResult.PASSED;
        } else if (memberState.isQuorum(acceptedNum.get() + notReadyTermNum.get())) {
            /**
             * 同意的节点+其他节点的选期小于当前节点的选期的节点数量达到合法数量
             *
             * 此处没有重新计算nextTimeToRequestVote  即再外层间隔10ms后   直接再次选举
             */
            parseResult = VoteResponse.ParseResult.REVOTE_IMMEDIATELY;
        } else if (memberState.isQuorum(acceptedNum.get() + biggerLedgerNum.get())) {
            parseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            nextTimeToRequestVote = getNextTimeToRequestVote();
        } else {
            parseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
            nextTimeToRequestVote = getNextTimeToRequestVote();
        }
        lastParseResult = parseResult;
        logger.info("[{}] [PARSE_VOTE_RESULT] cost={} term={} memberNum={} allNum={} acceptedNum={} notReadyTermNum={} biggerLedgerNum={} alreadyHasLeader={} maxTerm={} result={}",
            memberState.getSelfId(), lastVoteCost, term, memberState.peerSize(), allNum, acceptedNum, notReadyTermNum, biggerLedgerNum, alreadyHasLeader, knownMaxTermInGroup.get(), parseResult);

        /**
         * 选举通过
         */
        if (parseResult == VoteResponse.ParseResult.PASSED) {
            logger.info("[{}] [VOTE_RESULT] has been elected to be the leader in term {}", memberState.getSelfId(), term);
            /**
             * 当前节点晋升为leader
             */
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
        if (memberState.isLeader()) {
            maintainAsLeader();
        } else if (memberState.isFollower()) {
            maintainAsFollower();
        } else {
            maintainAsCandidate();
        }
    }

    /**
     * 通过 RoleChangeHandler 把角色变更透传给 RocketMQ 的Broker，从而达到主备自动切换的目标
     * @param term
     * @param role
     */
    private void handleRoleChange(long term, MemberState.Role role) {
        try {
            /**
             * 优选leader检查
             */
            takeLeadershipTask.check(term, role);
        } catch (Throwable t) {
            logger.error("takeLeadershipTask.check failed. ter={}, role={}", term, role, t);
        }

        for (RoleChangeHandler roleChangeHandler : roleChangeHandlers) {
            try {
                /**
                 * 接口由rocketmq实现
                 */
                roleChangeHandler.handle(term, role);
            } catch (Throwable t) {
                logger.warn("Handle role change failed term={} role={} handler={}", term, role, roleChangeHandler.getClass(), t);
            }
        }
    }

    public void addRoleChangeHandler(RoleChangeHandler roleChangeHandler) {
        if (!roleChangeHandlers.contains(roleChangeHandler)) {
            roleChangeHandlers.add(roleChangeHandler);
        }
    }

    /**
     * 主装让
     * @param request
     * @return
     * @throws Exception
     */
    public CompletableFuture<LeadershipTransferResponse> handleLeadershipTransfer(
        LeadershipTransferRequest request) throws Exception {
        logger.info("handleLeadershipTransfer: {}", request);
        synchronized (memberState) {
            /**
             * term不相等则跳出
             */
            if (memberState.currTerm() != request.getTerm()) {
                logger.warn("[BUG] [HandleLeaderTransfer] currTerm={} != request.term={}", memberState.currTerm(), request.getTerm());
                return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.INCONSISTENT_TERM.getCode()));
            }

            /**
             * 当前节点非leader  则跳出
             */
            if (!memberState.isLeader()) {
                logger.warn("[BUG] [HandleLeaderTransfer] selfId={} is not leader", request.getLeaderId());
                return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.NOT_LEADER.getCode()));
            }

            /**
             * 已经开始主装让交易
             */
            if (memberState.getTransferee() != null) {
                logger.warn("[BUG] [HandleLeaderTransfer] transferee={} is already set", memberState.getTransferee());
                return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.LEADER_TRANSFERRING.getCode()));
            }

            /**
             * 开启主转让标志
             */
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
            logger.warn("[HandleLeaderTransfer] term changed, cur={} , request={}", memberState.currTerm(), request.getTerm());
            return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.EXPIRED_TERM.getCode()));
        }

        /**
         * 通知优选节点
         */
        return dLedgerRpcService.leadershipTransfer(takeLeadershipRequest).thenApply(response -> {
            synchronized (memberState) {
                if (memberState.currTerm() == request.getTerm() && memberState.getTransferee() != null) {
                    logger.warn("leadershipTransfer failed, set transferee to null");
                    /**
                     * 关闭主装让标志
                     */
                    memberState.setTransferee(null);
                }
            }
            return response;
        });
    }

    /**
     *
     * @param request
     * @return
     * @throws Exception
     */
    public CompletableFuture<LeadershipTransferResponse> handleTakeLeadership(
        LeadershipTransferRequest request) throws Exception {
        logger.debug("handleTakeLeadership.request={}", request);
        synchronized (memberState) {
            if (memberState.currTerm() != request.getTerm()) {
                logger.warn("[BUG] [handleTakeLeadership] currTerm={} != request.term={}", memberState.currTerm(), request.getTerm());
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

        /**
         * 优选leader检查
         * @param term
         * @param role
         */
        public synchronized void check(long term, MemberState.Role role) {
            logger.trace("TakeLeadershipTask called, term={}, role={}", term, role);
            if (memberState.getTermToTakeLeadership() == -1 || responseFuture == null) {
                return;
            }
            LeadershipTransferResponse response = null;
            /**
             * term>termToTakeLeadership   则返回当前过去的term
             */
            if (term > memberState.getTermToTakeLeadership()) {
                response = new LeadershipTransferResponse().term(term).code(DLedgerResponseCode.EXPIRED_TERM.getCode());
            } else if (term == memberState.getTermToTakeLeadership()) {
                /**
                 * term==termToTakeLeadership
                 */
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
                /**
                 * term<termToTakeLeadership
                 */
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
            logger.info("TakeLeadershipTask finished. request={}, response={}, term={}, role={}", request, response, term, role);
            memberState.setTermToTakeLeadership(-1);
            responseFuture = null;
            request = null;
        }
    }

    /**
     * 该接口由具体业务实现   比如rocketmq
     */
    public interface RoleChangeHandler {
        void handle(long term, MemberState.Role role);

        void startup();

        void shutdown();
    }

    public class StateMaintainer extends ShutdownAbleThread {

        public StateMaintainer(String name, Logger logger) {
            super(name, logger);
        }

        @Override public void doWork() {
            try {
                if (DLedgerLeaderElector.this.dLedgerConfig.isEnableLeaderElector()) {
                    /**
                     * 设置时间间隔
                     */
                    DLedgerLeaderElector.this.refreshIntervals(dLedgerConfig);


                    DLedgerLeaderElector.this.maintainState();
                }

                /**
                 * 由ShutdownAbleThread的run方法可知    当前dowork   每10ms执行一次
                 */
                sleep(10);
            } catch (Throwable t) {
                DLedgerLeaderElector.logger.error("Error in heartbeat", t);
            }
        }

    }

}
