/**
 *
 */
package com.cielo.storage.fastdfs.codec;

import com.cielo.storage.fastdfs.FastdfsException;
import com.cielo.storage.fastdfs.FileId;
import com.cielo.storage.fastdfs.exchange.Replier;
import io.netty.buffer.ByteBuf;

import static com.cielo.storage.fastdfs.FastdfsConstants.FDFS_GROUP_LEN;
import static com.cielo.storage.fastdfs.FastdfsUtils.readString;

/**
 * 存储路径解码器
 *
 * @author liulongbiao
 */
public enum FileIdDecoder implements Replier.Decoder<FileId> {

    INSTANCE;

    @Override
    public FileId decode(ByteBuf in) {
        int length = in.readableBytes();
        if (length <= FDFS_GROUP_LEN) {
            throw new FastdfsException("body length : " + length + ", is lte required group name length 16.");
        }
        String group = readString(in, FDFS_GROUP_LEN);
        String path = readString(in);
        return new FileId(group, path);
    }

}