/*
 * Copyright 2017-2020 the original author or authors.
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

public class MemberState {

    public static final String TERM_PERSIST_FILE = "currterm";
    public static final String TERM_PERSIST_KEY_TERM = "currTerm";
    public static final String TERM_PERSIST_KEY_VOTE_FOR = "voteLeader";
    public static Logger logger = LoggerFactory.getLogger(MemberState.class);
    public final DLedgerConfig dLedgerConfig;
    private final ReentrantLock defaultLock = new ReentrantLock();
    private final String group;
    private final String selfId;
    private final String peers;
    private volatile Role role = CANDIDATE;
    private volatile String leaderId;
    private volatile long currTerm = 0;
    private volatile String currVoteFor;
    private volatile long ledgerEndIndex = -1;
    private volatile long ledgerEndTerm = -1;
    private long knownMaxTermInGroup = -1;
    private Map<String, String> peerMap = new HashMap<>();
    /**
     * 集群中得节点是否可用
     */
    private Map<String, Boolean> peersLiveTable = new ConcurrentHashMap<>();

    /**
     * 优选节点   当优选节点不为空  则表示已经开始主转让
     */
    private volatile String transferee;
    /**
     * 主转让时得term
     */
    private volatile long termToTakeLeadership = -1;

    public MemberState(DLedgerConfig config) {
        this.group = config.getGroup();
        this.selfId = config.getSelfId();
        this.peers = config.getPeers();
        /**
         * peers格式为：n0-localhost:20911；n1-xxx.xxx.xxx.xxx:xxxx ...
         *
         * 将其按照【;】和【-】分割后   注入到peerMap
         */
        for (String peerInfo : this.peers.split(";")) {
            String peerSelfId = peerInfo.split("-")[0];
            String peerAddress = peerInfo.substring(peerSelfId.length() + 1);
            peerMap.put(peerSelfId, peerAddress);
        }
        this.dLedgerConfig = config;

        /**
         * 加载term文件
         */
        loadTerm();
    }

    /**
     * 加载term
     */
    private void loadTerm() {
        try {
            /**
             * 读取自身节点下（selfId）的数据  \tmp\dledgerstore\dledger-n0\currterm
             */
            String data = IOUtils.file2String(dLedgerConfig.getDefaultPath() + File.separator + TERM_PERSIST_FILE);

            /**
             * 解析data成Properties
             */
            Properties properties = IOUtils.string2Properties(data);
            if (properties == null) {
                return;
            }

            /**
             * 从properties中获取currTerm
             */
            if (properties.containsKey(TERM_PERSIST_KEY_TERM)) {
                currTerm = Long.valueOf(String.valueOf(properties.get(TERM_PERSIST_KEY_TERM)));
            }

            /**
             * 从properties中获取voteLeader
             */
            if (properties.containsKey(TERM_PERSIST_KEY_VOTE_FOR)) {
                currVoteFor = String.valueOf(properties.get(TERM_PERSIST_KEY_VOTE_FOR));
                if (currVoteFor.length() == 0) {
                    currVoteFor = null;
                }
            }
        } catch (Throwable t) {
            logger.error("Load last term failed", t);
        }
    }

    /**
     * 持久化currterm文件
     */
    private void persistTerm() {
        try {
            /**
             * 设置文件对应的属性  voteLeader和currTerm
             */
            Properties properties = new Properties();
            properties.put(TERM_PERSIST_KEY_TERM, currTerm);
            properties.put(TERM_PERSIST_KEY_VOTE_FOR, currVoteFor == null ? "" : currVoteFor);

            /**
             * 将properties转成String
             */
            String data = IOUtils.properties2String(properties);

            /**
             * 持久化
             */
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

    /**
     * 选取currVoteFor为leader
     * @param currVoteFor
     */
    public synchronized void setCurrVoteFor(String currVoteFor) {
        this.currVoteFor = currVoteFor;

        /**
         * 持久化currterm文件
         */
        persistTerm();
    }

    /**
     * 更改选期并持久化到currterm文件
     * @return
     */
    public synchronized long nextTerm() {
        /**
         * 角色为CANDIDATE
         */
        PreConditions.check(role == CANDIDATE, DLedgerResponseCode.ILLEGAL_MEMBER_STATE, "%s != %s", role, CANDIDATE);

        /**
         * 集群内最大选期大于当前选期
         * 主转让时  会更新knownMaxTermInGroup   所以这里投票时会更新成主转让时得term
         */
        if (knownMaxTermInGroup > currTerm) {
            currTerm = knownMaxTermInGroup;
        } else {
            /**
             * 选期+1
             */
            ++currTerm;
        }
        currVoteFor = null;

        /**
         * 持久化currterm文件
         */
        persistTerm();
        return currTerm;
    }

    /**
     * 晋升为leader
     * @param term
     */
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

    /**
     * 修改节点角色为Candidate
     * @param term
     */
    public synchronized void changeToCandidate(long term) {
        assert term >= currTerm;
        PreConditions.check(term >= currTerm, DLedgerResponseCode.ILLEGAL_MEMBER_STATE, "should %d >= %d", term, currTerm);

        /**
         * 更新knownMaxTermInGroup为主转让得term
         */
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

    /**
     * 修改主转让时得term
     * @param termToTakeLeadership
     */
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

    /**
     * 是否达到法定席数  即传入的num是否大于等于(集群总量 / 2) + 1
     * @param num
     * @return
     */
    public boolean isQuorum(int num) {
        return num >= ((peerSize() / 2) + 1);
    }

    public int peerSize() {
        return peerMap.size();
    }

    /**
     * 是否为同一个集群
     */
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

    public enum Role {
        UNKNOWN,
        CANDIDATE,
        LEADER,
        FOLLOWER;
    }
}
