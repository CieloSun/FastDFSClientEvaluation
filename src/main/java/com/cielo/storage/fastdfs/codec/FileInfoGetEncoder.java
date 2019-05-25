package com.cielo.storage.fastdfs.codec;

import com.cielo.storage.fastdfs.FastdfsConstants;
import com.cielo.storage.fastdfs.FileId;

/**
 * @author siuming
 */
public class FileInfoGetEncoder extends FileIdOperationEncoder {

    /**
     * @param fileId
     */
    public FileInfoGetEncoder(FileId fileId) {
        super(fileId);
    }

    @Override
    protected byte cmd() {
        return FastdfsConstants.Commands.FILE_QUERY;
    }
}
