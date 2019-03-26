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

package io.openmessaging.storage.dledger.store.file;

import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MmapFileList {
    public static final int MIN_BLANK_LEN = 8;
    public static final int BLANK_MAGIC_CODE = -1;
    private static Logger logger = LoggerFactory.getLogger(MmapFile.class);
    private static final int DELETE_FILES_BATCH_MAX = 10;
    private final String storePath;

    private final int mappedFileSize;

    private final CopyOnWriteArrayList<MmapFile> mappedFiles = new CopyOnWriteArrayList<MmapFile>();

    private long flushedWhere = 0;
    private long committedWhere = 0;

    private volatile long storeTimestamp = 0;

    public MmapFileList(final String storePath, int mappedFileSize) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
    }

    /**
     * 校验文件是否联系
     * @return
     */
    public boolean checkSelf() {
        if (!this.mappedFiles.isEmpty()) {
            Iterator<MmapFile> iterator = mappedFiles.iterator();
            MmapFile pre = null;
            /**
             * 遍历问价你列表
             */
            while (iterator.hasNext()) {
                MmapFile cur = iterator.next();

                if (pre != null) {
                    /**
                     * 根绝相邻文件的FileFromOffset差值   判断文件是否连续
                     */
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        logger.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match pre file {}, cur file {}",
                            pre.getFileName(), cur.getFileName());
                        return false;
                    }
                }
                pre = cur;
            }
        }
        return true;
    }

    public MmapFile getMappedFileByTime(final long timestamp) {
        Object[] mfs = this.copyMappedFiles();

        if (null == mfs)
            return null;

        for (int i = 0; i < mfs.length; i++) {
            MmapFile mappedFile = (MmapFile) mfs[i];
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }

        return (MmapFile) mfs[mfs.length - 1];
    }

    private Object[] copyMappedFiles() {
        if (this.mappedFiles.size() <= 0) {
            return null;
        }
        return this.mappedFiles.toArray();
    }

    /**
     * 依照offset删除文件
     * @param offset
     */
    public void truncateOffset(long offset) {
        /**
         * 将列表转为数组
         */
        Object[] mfs = this.copyMappedFiles();
        if (mfs == null) {
            return;
        }

        /**
         * 获取将要移除的文件列表   凡是文件初始存储的offset大于指定offset的文件  都会被移除  offset所在的文件不会被删除
         */
        List<MmapFile> willRemoveFiles = new ArrayList<MmapFile>();

        for (int i = 0; i < mfs.length; i++) {
            MmapFile file = (MmapFile) mfs[i];
            /**
             * 每个文件可写入的最大offset
             */
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            if (fileTailOffset > offset) {
                if (offset >= file.getFileFromOffset()) {
                    /**
                     * 更新offset对应文件的WrotePosition、CommittedPosition、FlushedPosition
                     */
                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
                } else {
                    willRemoveFiles.add(file);
                }
            }
        }

        /**
         * 物理删除文件
         */
        this.destroyExpiredFiles(willRemoveFiles);
        /**
         * 缓存中删除文件
         */
        this.deleteExpiredFiles(willRemoveFiles);
    }

    /**
     * 删除files列表中的文件
     * @param files
     */
    void destroyExpiredFiles(List<MmapFile> files) {
        Collections.sort(files, new Comparator<MmapFile>() {
            @Override public int compare(MmapFile o1, MmapFile o2) {
                if (o1.getFileFromOffset() < o2.getFileFromOffset()) {
                    return -1;
                } else if (o1.getFileFromOffset() > o2.getFileFromOffset()) {
                    return 1;
                }
                return 0;
            }
        });

        for (int i = 0; i < files.size(); i++) {
            MmapFile mmapFile = files.get(i);
            while (true) {
                if (mmapFile.destroy(10 * 1000)) {
                    break;
                }
                DLedgerUtils.sleep(1000);
            }
        }
    }

    public void resetOffset(long offset) {
        Object[] mfs = this.copyMappedFiles();
        if (mfs == null) {
            return;
        }
        List<MmapFile> willRemoveFiles = new ArrayList<MmapFile>();

        for (int i = mfs.length - 1; i >= 0; i--) {
            MmapFile file = (MmapFile) mfs[i];
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            if (file.getFileFromOffset() <= offset) {
                if (offset < fileTailOffset) {
                    file.setStartPosition((int) (offset % this.mappedFileSize));
                } else {
                    willRemoveFiles.add(file);
                }
            }
        }

        this.destroyExpiredFiles(willRemoveFiles);
        this.deleteExpiredFiles(willRemoveFiles);
    }

    /**
     * 修改position
     * @param wherePosition
     */
    public void updateWherePosition(long wherePosition) {
        if (wherePosition > getMaxWrotePosition()) {
            logger.warn("[UpdateWherePosition] wherePosition {} > maxWrotePosition {}", wherePosition, getMaxWrotePosition());
            return;
        }

        /**
         * 修改刷盘位置
         */
        this.setFlushedWhere(wherePosition);
        /**
         * 修改提交位置
         */
        this.setCommittedWhere(wherePosition);
    }

    public long append(byte[] data) {
        return append(data, 0, data.length);
    }

    /**
     * 存储数据
     * @param data
     * @param pos
     * @param len
     * @return
     */
    public long append(byte[] data, int pos, int len) {
        return append(data, pos, len, true);
    }

    public long append(byte[] data, boolean useBlank) {
        return append(data, 0, data.length, useBlank);
    }

    /**
     * 预写入数据
     * @param len
     * @return
     */
    public long preAppend(int len) {
        return preAppend(len, true);
    }

    /**
     * 预写入数据   判断数据是否可以写入
     * @param len 预计添加的数据大小
     * @param useBlank 写入data文件为true  写入index为false（index的数据为定长）
     * @return
     */
    public long preAppend(int len, boolean useBlank) {
        /**
         * 获取最后一个文件
         */
        MmapFile mappedFile = getLastMappedFile();

        /**
         * 不存在或者已经写满则新建
         */
        if (null == mappedFile || mappedFile.isFull()) {
            mappedFile = getLastMappedFile(0);
        }
        if (null == mappedFile) {
            logger.error("Create mapped file for {}", storePath);
            return -1;
        }

        /**
         * 写入data文件为8 写入index为0（index的数据为定长）
         */
        int blank = useBlank ? MIN_BLANK_LEN : 0;

        /**
         * 待写入数据长度超过文件可写长度
         */
        if (len + blank > mappedFile.getFileSize() - mappedFile.getWrotePosition()) {
            /**
             * index文件  直接返回错误
             */
            if (blank < MIN_BLANK_LEN) {
                logger.error("Blank {} should ge {}", blank, MIN_BLANK_LEN);
                return -1;
            } else {
                /**
                 * data文件   写入BLANK_MAGIC_CODE  标志当前文件已不能再写入数据
                 */
                ByteBuffer byteBuffer = ByteBuffer.allocate(mappedFile.getFileSize() - mappedFile.getWrotePosition());
                byteBuffer.putInt(BLANK_MAGIC_CODE);
                byteBuffer.putInt(mappedFile.getFileSize() - mappedFile.getWrotePosition());

                /**
                 * 将数据写入到文件
                 */
                if (mappedFile.appendMessage(byteBuffer.array())) {
                    //need to set the wrote position
                    mappedFile.setWrotePosition(mappedFile.getFileSize());
                } else {
                    logger.error("Append blank error for {}", storePath);
                    return -1;
                }
                /**
                 * 创建新文件
                 */
                mappedFile = getLastMappedFile(0);
                if (null == mappedFile) {
                    logger.error("Create mapped file for {}", storePath);
                    return -1;
                }
            }
        }
        return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();

    }

    /**
     * 写入文件
     * @param data 数据
     * @param pos 读取位置
     * @param len 读取长度
     * @param useBlank  写入data文件为true  写入index为false（index的数据为定长）
     * @return
     */
    public long append(byte[] data, int pos, int len, boolean useBlank) {
        /**
         * 预写入数据   判断数据是否可以写入
         */
        if (preAppend(len, useBlank) == -1) {
            return -1;
        }
        MmapFile mappedFile = getLastMappedFile();
        long currPosition = mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();

        /**
         * 写入数据
         */
        if (!mappedFile.appendMessage(data, pos, len)) {
            logger.error("Append error for {}", storePath);
            return -1;
        }
        return currPosition;
    }

    /**
     * 获取offset处开始  长度为size得数据
     * @param offset
     * @param size
     * @return
     */
    public SelectMmapBufferResult getData(final long offset, final int size) {
        /**
         * 获取offset对应得mappedFile
         */
        MmapFile mappedFile = findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            /**
             * 获取数据
             */
            return mappedFile.selectMappedBuffer(pos, size);
        }
        return null;
    }

    /**
     * 获取offset所在的文件中   从offset开始的有效数据
     * @param offset
     * @return
     */
    public SelectMmapBufferResult getData(final long offset) {
        /**
         * 获取offset所在的文件
         */
        MmapFile mappedFile = findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.selectMappedBuffer(pos);
        }
        return null;
    }

    /**
     * 缓存中删除文件
     * @param files
     */
    void deleteExpiredFiles(List<MmapFile> files) {

        if (!files.isEmpty()) {

            /**
             * 去除不属于mappedFiles的文件
             */
            Iterator<MmapFile> iterator = files.iterator();
            while (iterator.hasNext()) {
                MmapFile cur = iterator.next();
                if (!this.mappedFiles.contains(cur)) {
                    iterator.remove();
                    logger.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
                }
            }

            try {
                /**
                 * 在mappedFiles中删除文件
                 */
                if (!this.mappedFiles.removeAll(files)) {
                    logger.error("deleteExpiredFiles remove failed.");
                }
            } catch (Exception e) {
                logger.error("deleteExpiredFiles has exception.", e);
            }
        }
    }

    /**
     * 加载data/index文件
     * @return
     */
    public boolean load() {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            Arrays.sort(files);
            /**
             * 遍历文件夹下的文件
             */
            for (File file : files) {
                /**
                 * 文件大小是否匹配
                 */
                if (file.length() != this.mappedFileSize) {
                    logger.warn(file + "\t" + file.length()
                        + " length not matched message store config value, please check it manually. You should delete old files before changing mapped file size");
                    return false;
                }
                try {
                    /**
                     * 按路径加载文件到内存
                     */
                    MmapFile mappedFile = new DefaultMmapFile(file.getPath(), mappedFileSize);

                    mappedFile.setWrotePosition(this.mappedFileSize);
                    mappedFile.setFlushedPosition(this.mappedFileSize);
                    mappedFile.setCommittedPosition(this.mappedFileSize);
                    this.mappedFiles.add(mappedFile);
                    logger.info("load " + file.getPath() + " OK");
                } catch (IOException e) {
                    logger.error("load file " + file + " error", e);
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * 获取最后一个文件   没有或者已经写满则创建新文件
     * @param startOffset
     * @param needCreate
     * @return
     */
    public MmapFile getLastMappedFile(final long startOffset, boolean needCreate) {
        long createOffset = -1;
        /**
         * 最后一个文件
         */
        MmapFile mappedFileLast = getLastMappedFile();

        /**
         * 当最后一个文件不存在或者已经写满时   得到下一个文件的名字
         */
        if (mappedFileLast == null) {
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        } else if (mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }

        /**
         * 创建新文件
         */
        if (createOffset != -1 && needCreate) {
            /**
             * 新文件路径
             */
            String nextFilePath = this.storePath + File.separator + DLedgerUtils.offset2FileName(createOffset);
            MmapFile mappedFile = null;
            try {
                /**
                 * 创建并加载文件到内存
                 */
                mappedFile = new DefaultMmapFile(nextFilePath, this.mappedFileSize);
            } catch (IOException e) {
                logger.error("create mappedFile exception", e);
            }

            if (mappedFile != null) {
                if (this.mappedFiles.isEmpty()) {
                    /**
                     * 赋予第一个文件标志
                     */
                    mappedFile.setFirstCreateInQueue(true);
                }
                this.mappedFiles.add(mappedFile);
            }

            return mappedFile;
        }

        return mappedFileLast;
    }

    /**
     * 获取最后一个文件
     * @param startOffset
     * @return
     */
    public MmapFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    /**
     * 获取文件列表中的最后一个文件
     * @return
     */
    public MmapFile getLastMappedFile() {
        MmapFile mappedFileLast = null;

        while (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                logger.error("getLastMappedFile has exception.", e);
                break;
            }
        }

        return mappedFileLast;
    }

    public long getMinOffset() {
        MmapFile mmapFile = getFirstMappedFile();
        if (mmapFile != null) {
            return mmapFile.getFileFromOffset() + mmapFile.getStartPosition();
        }
        return 0;
    }

    public long getMaxReadPosition() {
        MmapFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }

    /**
     * 获取文件列表中的最大写入位置
     * @return
     */
    public long getMaxWrotePosition() {
        /**
         * 获取最后一个文件
         */
        MmapFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            /**
             * 返回最大写入位置
             */
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }

    public long remainHowManyDataToCommit() {
        return getMaxWrotePosition() - committedWhere;
    }

    public long remainHowManyDataToFlush() {
        return getMaxReadPosition() - flushedWhere;
    }

    public void deleteLastMappedFile() {
        MmapFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            lastMappedFile.destroy(1000);
            this.mappedFiles.remove(lastMappedFile);
            logger.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());

        }
    }

    public int deleteExpiredFileByTime(final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately) {
        Object[] mfs = this.copyMappedFiles();

        if (null == mfs)
            return 0;

        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        List<MmapFile> files = new ArrayList<MmapFile>();
        if (null != mfs) {
            for (int i = 0; i < mfsLength; i++) {
                MmapFile mappedFile = (MmapFile) mfs[i];
                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
                if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
                    if (mappedFile.destroy(intervalForcibly)) {
                        files.add(mappedFile);
                        deleteCount++;

                        if (files.size() >= DELETE_FILES_BATCH_MAX) {
                            break;
                        }

                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException e) {
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    //avoid deleting files in the middle
                    break;
                }
            }
        }

        deleteExpiredFiles(files);

        return deleteCount;
    }

    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        Object[] mfs = this.copyMappedFiles();

        List<MmapFile> files = new ArrayList<MmapFile>();
        int deleteCount = 0;
        if (null != mfs) {

            int mfsLength = mfs.length - 1;

            for (int i = 0; i < mfsLength; i++) {
                boolean destroy;
                MmapFile mappedFile = (MmapFile) mfs[i];
                SelectMmapBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    result.release();
                    destroy = maxOffsetInLogicQueue < offset;
                    if (destroy) {
                        logger.info("physic min offset " + offset + ", logics in current mappedFile max offset "
                            + maxOffsetInLogicQueue + ", delete it");
                    }
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    logger.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    logger.warn("this being not executed forever.");
                    break;
                }

                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        deleteExpiredFiles(files);

        return deleteCount;
    }

    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        MmapFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
        if (mappedFile != null) {
            int offset = mappedFile.flush(flushLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.flushedWhere;
            this.flushedWhere = where;
        }

        return result;
    }

    public boolean commit(final int commitLeastPages) {
        boolean result = true;
        MmapFile mappedFile = this.findMappedFileByOffset(this.committedWhere, this.committedWhere == 0);
        if (mappedFile != null) {
            int offset = mappedFile.commit(commitLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.committedWhere;
            this.committedWhere = where;
        }

        return result;
    }

    /**
     * Finds a mapped file by offset.
     *
     * @param offset Offset.
     * @param returnFirstOnNotFound If the mapped file is not found, then return the first one.
     * @return Mapped file or null (when not found and returnFirstOnNotFound is <code>false</code>).
     */
    public MmapFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            MmapFile firstMappedFile = this.getFirstMappedFile();
            MmapFile lastMappedFile = this.getLastMappedFile();
            if (firstMappedFile != null && lastMappedFile != null) {
                if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
                    logger.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
                        offset,
                        firstMappedFile.getFileFromOffset(),
                        lastMappedFile.getFileFromOffset() + this.mappedFileSize,
                        this.mappedFileSize,
                        this.mappedFiles.size());
                } else {
                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                    MmapFile targetFile = null;
                    try {
                        targetFile = this.mappedFiles.get(index);
                    } catch (Exception ignored) {
                    }

                    if (targetFile != null && offset >= targetFile.getFileFromOffset()
                        && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
                        return targetFile;
                    }

                    logger.warn("Offset is matched, but get file failed, maybe the file number is changed. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
                        offset,
                        firstMappedFile.getFileFromOffset(),
                        lastMappedFile.getFileFromOffset() + this.mappedFileSize,
                        this.mappedFileSize,
                        this.mappedFiles.size());

                    for (MmapFile tmpMappedFile : this.mappedFiles) {
                        if (offset >= tmpMappedFile.getFileFromOffset()
                            && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
                            return tmpMappedFile;
                        }
                    }
                }

                if (returnFirstOnNotFound) {
                    return firstMappedFile;
                }
            }
        } catch (Exception e) {
            logger.error("findMappedFileByOffset Exception", e);
        }

        return null;
    }

    public MmapFile getFirstMappedFile() {
        MmapFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                logger.error("getFirstMappedFile has exception.", e);
            }
        }

        return mappedFileFirst;
    }

    public MmapFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    public long getMappedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMappedFiles();
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }

        return size;
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MmapFile mappedFile = this.getFirstMappedFile();
        if (mappedFile != null) {
            if (!mappedFile.isAvailable()) {
                logger.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    logger.info("the mappedFile re delete OK, " + mappedFile.getFileName());
                    List<MmapFile> tmpFiles = new ArrayList<MmapFile>();
                    tmpFiles.add(mappedFile);
                    this.deleteExpiredFiles(tmpFiles);
                } else {
                    logger.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }

    public void shutdown(final long intervalForcibly) {
        for (MmapFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    public void destroy() {
        for (MmapFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.flushedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }

    public boolean rebuildWithPos(long pos) {
        truncateOffset(-1);
        getLastMappedFile(pos);
        truncateOffset(pos);
        resetOffset(pos);
        return pos == getMaxWrotePosition() && pos == getMinOffset();
    }

    public long getFlushedWhere() {
        return flushedWhere;
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public List<MmapFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public long getCommittedWhere() {
        return committedWhere;
    }

    public void setCommittedWhere(final long committedWhere) {
        this.committedWhere = committedWhere;
    }
}
