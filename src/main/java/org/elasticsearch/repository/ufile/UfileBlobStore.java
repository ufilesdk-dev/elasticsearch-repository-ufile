package org.elasticsearch.repository.ufile;

import cn.ucloud.ufile.bean.ObjectInfoBean;
import cn.ucloud.ufile.bean.ObjectListBean;
import cn.ucloud.ufile.exception.UfileClientException;
import cn.ucloud.ufile.exception.UfileServerException;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.blobstore.*;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;

import java.io.*;
import java.util.*;


public class UfileBlobStore implements BlobStore {
    private final Logger logger = LogManager.getLogger(UfileBlobStore.class);
    private final UfileService client;
    private final String bucket;

    UfileBlobStore(Settings settings, String bucket, UfileService client) {

        this.client = client;
        this.bucket = bucket;
        boolean exist = doesBucketExist(bucket);
        if (!exist) {
            throw new BlobStoreException("Bucket [" + bucket + "] does not exist");
        }
    }

    String getBucket() {
        return this.bucket;
    }

    @Override
    public BlobContainer blobContainer(BlobPath blobPath) {
        return new UfileBlobContainer(blobPath, this);
    }

    //按目录删除文件
    @Override
    public void delete(BlobPath blobPath) throws IOException {
        SocketAccess.doPrivilegedIOException(() -> {
            logger.debug("delete path: {}", blobPath.buildAsString());
            Map<String, BlobMetaData> blobs = listBlobsByPrefix(blobPath.buildAsString(), null);
            Iterator<String> blobNameIterator = blobs.keySet().iterator();
            while (blobNameIterator.hasNext()) {
                String blobName = blobNameIterator.next();

                try {
                    this.client.deleteObject(bucket, blobPath.buildAsString() + blobName);
                } catch (UfileClientException e) {
                    logger.error("UfileBlobStore.delete.UfileClientException: [{}]", e.getMessage());
                    throw new IOException(e.getMessage());
                } catch (UfileServerException e) {
                    logger.error("UfileBlobStore.delete.UfileServerException: [{}]", e.getMessage());
                    throw new IOException(e.getMessage());
                }
            }
            return null;
        });
    }

    Map<String, BlobMetaData> listBlobsByPrefix(String keyPath, String prefix) throws IOException {
        return SocketAccess.doPrivilegedIOException(() -> {
            MapBuilder<String, BlobMetaData> blobsBuilder = MapBuilder.newMapBuilder();
            String actualPrefix = keyPath + (prefix == null ? StringUtils.EMPTY : prefix);
            String nextMarker = null;
            ObjectListBean blobs;
            do {
                blobs = this.client.listObjects(bucket, actualPrefix, nextMarker);
                for (ObjectInfoBean objInfo : blobs.getObjectList()) {
                    String blobName = objInfo.getFileName().substring(keyPath.length());
                    blobsBuilder.put(blobName, new PlainBlobMetaData(blobName, objInfo.getSize()));
                }
                nextMarker = blobs.getNextMarker();
                logger.debug("bucket [{}], path [{}], prefix [{}], nextMarker [{}] [{}][{}] [{}]", bucket, keyPath, prefix, nextMarker, (nextMarker == ""), (nextMarker == null), nextMarker.length());
            } while (nextMarker != null && nextMarker.length() != 0);
            return blobsBuilder.immutableMap();
        });
    }

    @Override
    public void close() throws IOException {
        client.shutdown();
    }

    //判断bucket 存在性
    private boolean doesBucketExist(String bucketName) {
        boolean b = this.client.doesBucketExist(bucketName);
        return b;
    }

    //对象存在性
    boolean blobExists(String blobName) throws IOException {
        return SocketAccess.doPrivilegedIOException(() -> {
            try {
                boolean r = this.client.doesObjectExist(bucket, blobName);
                logger.debug("UfileBlobStore.blobExists, exist: [{}]", r);
                return r;
            } catch (UfileClientException e) {
                logger.error("UfileBlobStore.blobExists.UfileClientException: [{}]", e.getMessage());
                throw new IOException(e.getMessage());
            } catch (UfileServerException e) {
                logger.error("UfileBlobStore.blobExists.UfileServerException: [{}]", e.getMessage());
                throw new IOException(e.getMessage());
            }
        });
    }

    //读取对象
    InputStream readBlob(String blobName) throws IOException {
        return SocketAccess.doPrivilegedIOException(() -> {
            try {
                InputStream ins = this.client.getObject(bucket, blobName).getInputStream();
                return ins;
            } catch (UfileClientException e) {
                logger.error("UfileBlobStore.move.UfileClientException: [{}]", e.getMessage());
                throw new IOException(e.getMessage());
            } catch (UfileServerException e) {
                logger.error("UfileBlobStore.move.UfileServerException: [{}]", e.getMessage());
                throw new IOException(e.getMessage());
            }
        });
    }


    void writeBlob(String blobName, InputStream inputStream, long blobSize) throws IOException {
        SocketAccess.doPrivilegedIOException(() -> {
            try {
                byte[] buf = new byte[(int) (blobSize)];
                int read = inputStream.read(buf);
                logger.debug("writeBlob blobSize:[{}], readSize[{}]", blobSize, read);
                InputStream inputStream2 = new ByteArrayInputStream(buf);
                logger.debug("writeBlob inputStrem size:[{}]", inputStream2.available());
                this.client.putObject(bucket, blobName, inputStream2, blobSize);
            } catch (UfileClientException e) {
                logger.error("UfileBlobStore.writeBlob.UfileClientException: [{}]", e.getMessage());
                throw new IOException(e.getMessage());
            } catch (UfileServerException e) {
                logger.error("UfileBlobStore.writeBlob.UfileServerException: [{}]", e.getMessage());
                throw new IOException(e.getMessage());
            }
            return null;
        });
    }

    //删除对象
    void deleteBlob(String blobName) throws IOException {
        SocketAccess.doPrivilegedIOException(() -> {
            try {
                this.client.deleteObject(bucket, blobName);
            } catch (UfileClientException e) {
                logger.error("UfileBlobStore.deleteBlob.UfileClientException: [{}]", e.getMessage());
                throw new IOException(e.getMessage());
            } catch (UfileServerException e) {
                logger.error("UfileBlobStore.deleteBlob.UfileServerException: [{}]", e.getMessage());
                throw new IOException(e.getMessage());
            }
            return null;
        });
    }

    //移动对象
    public void move(String sourceBlobName, String targetBlobName) throws IOException {
        SocketAccess.doPrivilegedIOException(() -> {
            try {
                this.client.copyObject(bucket, sourceBlobName, bucket, targetBlobName);
                this.client.deleteObject(bucket, sourceBlobName);
            } catch (UfileClientException e) {
                logger.error("UfileBlobStore.move.UfileClientException: [{}]", e.getMessage());
                throw new IOException(e.getMessage());
            } catch (UfileServerException e) {
                logger.error("UfileBlobStore.move.UfileServerException: [{}]", e.getMessage());
                throw new IOException(e.getMessage());
            }
            return null;
        });
    }

}
