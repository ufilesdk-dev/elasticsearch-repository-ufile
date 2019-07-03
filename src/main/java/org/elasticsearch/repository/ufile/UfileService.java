package org.elasticsearch.repository.ufile;

import cn.ucloud.ufile.bean.DownloadStreamBean;
import cn.ucloud.ufile.bean.ObjectListBean;
import cn.ucloud.ufile.exception.UfileClientException;
import cn.ucloud.ufile.exception.UfileServerException;

import java.io.InputStream;


public interface UfileService {
    boolean doesObjectExist(String bucketName, String key)
            throws UfileServerException, UfileClientException;

    boolean doesBucketExist(String bucketName);

    ObjectListBean listObjects(String bucketName, String prefix, String marker)
            throws UfileServerException, UfileClientException;

    DownloadStreamBean getObject(String bucketName, String key)
            throws UfileServerException, UfileClientException;

    void putObject(String bucketName, String key, InputStream input, long blobSize)
            throws UfileServerException, UfileClientException;

    void deleteObject(String bucketName, String key)
            throws UfileServerException, UfileClientException;

    void copyObject(String sourceBucketName, String sourceKey,
                    String destinationBucketName, String destinationKey)
            throws UfileServerException, UfileClientException;

    void shutdown();
}
