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

package io.openmessaging.storage.dledger.protocol;

import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.utils.PreConditions;
import java.util.ArrayList;
import java.util.List;

public class PushEntryRequest extends RequestOrResponse {
    private long commitIndex = -1;
    private Type type = Type.APPEND;
    private DLedgerEntry entry;

    //for batch append push
    private List<DLedgerEntry> batchEntry = new ArrayList<>();
    private int totalSize;

    public DLedgerEntry getEntry() {
        return entry;
    }

    public void setEntry(DLedgerEntry entry) {
        this.entry = entry;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    /**
     * 添加批量数据
     * @param entry
     */
    public void addEntry(DLedgerEntry entry) {
        /**
         * 保证数据连续
         */
        if (!batchEntry.isEmpty()) {
            PreConditions.check(batchEntry.get(0).getIndex() + batchEntry.size() == entry.getIndex(),
                DLedgerResponseCode.UNKNOWN, "batch push wrong order");
        }
        /**
         * 缓存带append数据
         */
        batchEntry.add(entry);
        /**
         * 总数据大小
         */
        totalSize += entry.getSize();
    }

    /**
     * 获取第一条数据得index
     * @return
     */
    public long getFirstEntryIndex() {
        if (!batchEntry.isEmpty()) {
            return batchEntry.get(0).getIndex();
        } else {
            return -1;
        }
    }

    /**
     * 获取最后一条数据的index
     * @return
     */
    public long getLastEntryIndex() {
        if (!batchEntry.isEmpty()) {
            return batchEntry.get(batchEntry.size() - 1).getIndex();
        } else {
            return -1;
        }
    }

    /**
     * 发送得数据个数
     * @return
     */
    public int getCount() {
        return batchEntry.size();
    }

    public long getTotalSize() {
        return totalSize;
    }

    public List<DLedgerEntry> getBatchEntry() {
        return batchEntry;
    }

    /**
     * 清空批量数据
     */
    public void clear() {
        batchEntry.clear();
        totalSize = 0;
    }

    public enum Type {
        APPEND,
        COMMIT,
        COMPARE,
        TRUNCATE
    }
}
