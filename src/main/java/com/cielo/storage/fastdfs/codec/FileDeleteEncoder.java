/**
 *
 */
package com.cielo.storage.fastdfs.codec;

import com.cielo.storage.fastdfs.FastdfsConstants;
import com.cielo.storage.fastdfs.FileId;

/**
 * 删除请求
 *
 * @author liulongbiao
 */
public class FileDeleteEncoder extends FileIdOperationEncoder {

    public FileDeleteEncoder(FileId fileId) {
        super(fileId);
    }

    @Override
    protected byte cmd() {
        return FastdfsConstants.Commands.FILE_DELETE;
    }
}
