package org.elasticsearch.repository.ufile;

import cn.ucloud.ufile.api.object.ObjectApiBuilder;
import cn.ucloud.ufile.api.object.ObjectConfig;
import cn.ucloud.ufile.api.object.multi.MultiUploadInfo;
import cn.ucloud.ufile.api.object.multi.MultiUploadPartState;
import cn.ucloud.ufile.auth.ObjectAuthorization;
import cn.ucloud.ufile.auth.UfileObjectLocalAuthorization;
import cn.ucloud.ufile.exception.UfileClientException;
import cn.ucloud.ufile.exception.UfileServerException;
import cn.ucloud.ufile.UfileClient;
import cn.ucloud.ufile.bean.*;
import cn.ucloud.ufile.util.FileUtil;
import com.google.gson.Gson;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UfileServiceImpl extends AbstractComponent implements UfileService {
    private ObjectApiBuilder client;

    public UfileServiceImpl(Settings settings, RepositoryMetaData metaData) {
        this.client = createClient(metaData);
    }

    private ObjectApiBuilder createClient(RepositoryMetaData metaData) {
        ObjectApiBuilder client;

        String accessKeyId = UfileClientSettings.PUBLIC_KEY.get(metaData.settings());

        String secretAccessKey = UfileClientSettings.PRIVATE_KEY.get(metaData.settings());

        String endpoint = UfileClientSettings.ENDPOINT.get(metaData.settings());

        //构造签名对象
        ObjectAuthorization OBJECT_AUTHORIZER = new UfileObjectLocalAuthorization(accessKeyId, secretAccessKey);

        ObjectConfig config = new ObjectConfig(endpoint);

        client = UfileClient.object(OBJECT_AUTHORIZER, config);
        return client;
    }

    //判断文件是否存在
    @Override
    public boolean doesObjectExist(String bucketName, String key) throws UfileServerException, UfileClientException {
        try {
            logger.debug("UfileServiceImpl.doesObjectExist, bucket:[{}], key:[{}]", bucketName, key);
            ObjectProfile response = this.client.objectProfile(key, bucketName).execute();
        } catch (UfileServerException e) {
            //api没有提供方法获取responsecode 反序列化exception message来获取。
            UfileErrorBean errorBean = new Gson().fromJson(e.getMessage(), UfileErrorBean.class);
            int code = errorBean.getResponseCode();
            if (code == 404) {
                //404表示对象不存在 现有api只能这么处理，之后考虑改掉
                logger.debug("UfileServiceImpl.doesObjectExist: [{}]", e.toString());
            } else {
                logger.error("UfileServiceImpl.doesObjectExist: [{}]", e.toString());
            }
            return false;
        }
        return true;
    }

    //判断bucket是否存在
    @Override
    public boolean doesBucketExist(String bucketName) {
        try {
            logger.debug("UfileServiceImpl.doesBucketExist, bucket:[{}]", bucketName);
            this.client.objectList(bucketName)
                    .withPrefix("")
                    .withMarker("")
                    .dataLimit(1)
                    .execute();
            logger.debug("UfileServiceImpl.doesBucketExist: true");
            return true;
        } catch (UfileClientException e) {
            logger.error("UfileServiceImpl.doesBucketExist.UfileClientException: [{}]", e.getMessage());
            return false;
        } catch (UfileServerException e) {
            logger.error("UfileServiceImpl.doesBucketExist.UfileServerException: [{}]", e.getMessage());
            return false;
        }
    }

    //列表文件
    @Override
    public ObjectListBean listObjects(String bucketName, String prefix, String marker) throws UfileServerException, UfileClientException {
        logger.debug("UfileServiceImpl.listObjects, bucket:[{}], prefix:[{}], marker:[{}]", bucketName, prefix, marker);
        return this.client.objectList(bucketName)
                .withPrefix(prefix)
                .withMarker(marker)
                .dataLimit(50)
                .execute();
    }

    //下载文件
    @Override
    public DownloadStreamBean getObject(String bucketName, String key) throws UfileServerException, UfileClientException {
        logger.debug("UfileServiceImpl.getObject, bucket:[{}], key:[{}]", bucketName, key);
        String url = this.client.getDownloadUrlFromPrivateBucket(key, bucketName, 30 * 60)
                .createUrl();
        DownloadStreamBean down_bean = this.client.getStream(url).execute();
        logger.debug("UfileServiceImpl.getObject, bucket:[{}], key:[{}], len:[{}]", bucketName, key, down_bean.getContentLength());
        return down_bean;
    }

    //上传文件
    @Override
    public void putObject(String bucketName, String key, InputStream input, long blobSize) throws UfileServerException, UfileClientException {
        logger.debug("UfileServiceImpl.putObject, bucket:[{}], key:[{}], size:[{}]", bucketName, key, blobSize);

//        String mineType = MimeTypeUtil.getMimeType(new File(key));
        String mimeType = "application/octet-stream";
        long mputThreshold = 10 << 10 << 10; //10m
        if (blobSize < mputThreshold) {
            //用put
            this.client.putObject(input, mimeType).nameAs(key).toBucket(bucketName).execute();
            return;
        } else {
            //用mput
            // 先初始化分片上环请求
            logger.debug("UfileServiceImpl.initMultiUpload");
            MultiUploadInfo upload_info = this.client.initMultiUpload(key, mimeType, bucketName).execute();
            if (upload_info == null)
                throw new UfileServerException("upload init null");

            logger.debug("UfileServiceImpl.multiUpload");
            List<MultiUploadPartState> partStates = multiUpload(input, upload_info);
            // 若上传分片结果列表为空，则失败，需中断上传操作。否则完成上传
            if (partStates == null || partStates.isEmpty()) {
                //失败
                logger.debug("UfileServiceImpl.abortMultiUpload");
                this.client.abortMultiUpload(upload_info).execute();
            } else {
                //成功
                logger.debug("UfileServiceImpl.finishMultiUpload");
                this.client.finishMultiUpload(upload_info, partStates).execute();
            }
            return;
        }
    }

    public List<MultiUploadPartState> multiUpload(InputStream is, MultiUploadInfo upload_info) throws UfileClientException {
        logger.debug("UfileServiceImpl.multiUpload, bucket:[{}], key:[{}]", upload_info.getBucket(), upload_info.getKeyName());
        List<MultiUploadPartState> part_states = null;
        byte[] buffer = new byte[upload_info.getBlkSize()];
        try {
            int len = 0;
            int count = 0;
            part_states = new ArrayList<>();
            // 将数据根据state中指定的大小进行分片
            while ((len = is.read(buffer)) > 0) {
                final int index = count++;
                byte[] sendData = Arrays.copyOf(buffer, len);
                int uploadCount = 0;

                // 可支持重试3次上传
                while (uploadCount < 3) {
                    try {
                        MultiUploadPartState part_state = this.client
                                .multiUploadPart(upload_info, sendData, index)
                                .setOnProgressListener(null)
                                .execute();
                        if (part_state == null) {
                            uploadCount++;
                            continue;
                        }

                        part_states.add(part_state);
                        break;
                    } catch (UfileServerException e) {
                        // 尝试次数+1
                        uploadCount++;
                        logger.error("mput upload fail, retry times {}. Exception info {}", uploadCount, e.getMessage());
                    }
                }
                if (uploadCount == 3)
                    return null;
            }
            return part_states;
        } catch (IOException e) {
            throw new UfileClientException(e.getMessage());
        } finally {
            FileUtil.close(is);
        }
    }

    //删除文件
    @Override
    public void deleteObject(String bucketName, String key) throws UfileServerException, UfileClientException {
        logger.debug("UfileServiceImpl.deleteObject, bucket:[{}], key:[{}]", bucketName, key);
        this.client.deleteObject(key, bucketName).execute();
    }

    //复制文件
    @Override
    public void copyObject(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey) throws UfileServerException, UfileClientException {
        logger.debug("UfileServiceImpl.copyObject, src_bucket:[{}], src_key:[{}], dst_bucket:[{}], dst_key:[{}]", sourceBucketName, sourceKey, destinationBucketName, destinationKey);

        String url = this.client.getDownloadUrlFromPrivateBucket(sourceKey, sourceBucketName, 30 * 60)
                .createUrl();

        DownloadStreamBean down_bean;
        try {
            down_bean = this.client.getStream(url).execute();
        } catch (UfileServerException | UfileClientException e) {
            logger.error("download exception [{}]", e.getMessage());
            throw e;
        }
        try {
            this.putObject(destinationBucketName, destinationKey, down_bean.getInputStream(), down_bean.getContentLength());
//            String mine_type = down_bean.getContentType();
//            this.client.putObject(down_bean.getInputStream(), mine_type).nameAs(destinationKey).toBucket(destinationBucketName).execute();
        } catch (UfileServerException | UfileClientException e) {
            logger.error("upload exception [{}]", e.getMessage());
            throw e;
        }
        return;
    }

    @Override
    public void shutdown() {
    }
}
