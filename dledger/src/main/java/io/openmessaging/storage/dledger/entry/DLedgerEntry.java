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

package io.openmessaging.storage.dledger.entry;

/**
 * DLedger日志存储实例，一个对象代表一个日志条目
 */
public class DLedgerEntry {

    public final static int POS_OFFSET = 4 + 4 + 8 + 8;
    public final static int HEADER_SIZE = POS_OFFSET + 8 + 4 + 4 + 4;
    public final static int BODY_OFFSET = HEADER_SIZE + 4;

    /**
     * 魔数
     */
    private int magic = DLedgerEntryType.NORMAL.getMagic();
    /**
     * 条目总长度，包含header（协议头）+body（消息体），占4字节。
     */
    private int size;
    /**
     * 当前条目的日志序号，占8字节。
     */
    private long index;
    /**
     * 条目所属的投票轮次，占8字节。
     */
    private long term;
    /**
     * 条目的物理偏移量，类似CommitLog文件的物理偏移量，占8字节。
     */
    private long pos; //used to validate data
    /**
     * 保留字段，当前版本未使用，占4字节。
     */
    private int channel; //reserved
    /**
     * 当前版本未使用，占4字节。
     */
    private int chainCrc; //like the block chain, this crc indicates any modification before this entry.
    /**
     * 消息体的CRC校验和，用来区分数据是否损坏，占4字节
     */
    private int bodyCrc; //the crc of the body
    /**
     * 消息体的内容。
     */
    private byte[] body;

    public DLedgerEntry() {

    }

    public DLedgerEntry(DLedgerEntryType type) {
        this.magic = type.getMagic();
        if (type == DLedgerEntryType.NOOP) {
            this.body = new byte[0];
        }
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getMagic() {
        return magic;
    }

    public void setMagic(int magic) {
        this.magic = magic;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public int getChainCrc() {
        return chainCrc;
    }

    public void setChainCrc(int chainCrc) {
        this.chainCrc = chainCrc;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public int getBodyCrc() {
        return bodyCrc;
    }

    public void setBodyCrc(int bodyCrc) {
        this.bodyCrc = bodyCrc;
    }

    public int computeSizeInBytes() {
        size = HEADER_SIZE + 4 + body.length;
        return size;
    }

    public long getPos() {
        return pos;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }

    @Override
    public boolean equals(Object entry) {
        if (entry == null || !(entry instanceof DLedgerEntry)) {
            return false;
        }
        DLedgerEntry other = (DLedgerEntry) entry;
        if (this.size != other.size
            || this.magic != other.magic
            || this.index != other.index
            || this.term != other.term
            || this.channel != other.channel
            || this.pos != other.pos) {
            return false;
        }
        if (body == null) {
            return other.body == null;
        }

        if (other.body == null) {
            return false;
        }
        if (body.length != other.body.length) {
            return false;
        }
        for (int i = 0; i < body.length; i++) {
            if (body[i] != other.body[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int h = 1;
        h = prime * h + size;
        h = prime * h + magic;
        h = prime * h + (int) index;
        h = prime * h + (int) term;
        h = prime * h + channel;
        h = prime * h + (int) pos;
        if (body != null) {
            for (int i = 0; i < body.length; i++) {
                h = prime * h + body[i];
            }
        } else {
            h = prime * h;
        }
        return h;
    }

    public int getChannel() {
        return channel;
    }

    public void setChannel(int channel) {
        this.channel = channel;
    }
}
