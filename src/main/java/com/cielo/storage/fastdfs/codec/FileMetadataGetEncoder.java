/**
 *
 */
package com.cielo.storage.fastdfs.codec;

import com.cielo.storage.fastdfs.FastdfsConstants;
import com.cielo.storage.fastdfs.FileId;

/**
 * 获取文件属性请求
 *
 * @author liulongbiao
 */
public class FileMetadataGetEncoder extends FileIdOperationEncoder {

    public FileMetadataGetEncoder(FileId fileId) {
        super(fileId);
    }

    @Override
    public byte cmd() {
        return FastdfsConstants.Commands.METADATA_GET;
    }

}