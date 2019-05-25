/**
 *
 */
package com.cielo.storage.fastdfs.codec;

import com.cielo.storage.fastdfs.FileId;

import static com.cielo.storage.fastdfs.FastdfsConstants.Commands.SERVICE_QUERY_UPDATE;

/**
 * 获取可更新的存储服务器
 *
 * @author liulongbiao
 */
public class UpdateStorageGetEncoder extends FileIdOperationEncoder {

    public UpdateStorageGetEncoder(FileId fileId) {
        super(fileId);
    }

    @Override
    protected byte cmd() {
        return SERVICE_QUERY_UPDATE;
    }

}
