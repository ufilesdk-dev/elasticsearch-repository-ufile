package org.elasticsearch.ucloud.ufile.service;

import cn.ucloud.ufile.api.object.ObjectApiBuilder;
import cn.ucloud.ufile.api.object.ObjectConfig;
import cn.ucloud.ufile.api.object.multi.MultiUploadInfo;
import cn.ucloud.ufile.api.object.multi.MultiUploadPartState;
import cn.ucloud.ufile.auth.ObjectAuthorization;
import cn.ucloud.ufile.auth.UfileObjectLocalAuthorization;
import cn.ucloud.ufile.bean.base.BaseResponseBean;
import cn.ucloud.ufile.exception.UfileClientException;
import cn.ucloud.ufile.exception.UfileServerException;
import cn.ucloud.ufile.UfileClient;
import cn.ucloud.ufile.bean.*;
import cn.ucloud.ufile.util.FileUtil;
import cn.ucloud.ufile.util.MimeTypeUtil;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repository.ufile.UfileRepository;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UfileServiceImpl extends AbstractComponent implements UfileService {
    private ObjectApiBuilder client;

    public UfileServiceImpl(Settings settings, RepositoryMetaData metadata) {
        super(settings);
        this.client = createClient(metadata);
    }

    private ObjectApiBuilder createClient(RepositoryMetaData repositoryMetaData) {
        ObjectApiBuilder client;

        String accessKeyId =
            UfileRepository.getSetting(UfileClientSettings.ACCESS_KEY_ID, repositoryMetaData);
        String secretAccessKey =
            UfileRepository.getSetting(UfileClientSettings.SECRET_ACCESS_KEY, repositoryMetaData);
        String endpoint = UfileRepository.getSetting(UfileClientSettings.ENDPOINT, repositoryMetaData);
        // Using the specified Ufile Endpoint, temporary Token information provided by the STS
        String securityToken = UfileClientSettings.SECURITY_TOKEN.get(repositoryMetaData.settings());

        //构造签名对象
        ObjectAuthorization OBJECT_AUTHORIZER = new UfileObjectLocalAuthorization(accessKeyId, secretAccessKey);
//        ObjectConfig config = new ObjectConfig("cn-bj", "ufileos.com");
        ObjectConfig config = new ObjectConfig(endpoint);

        client = UfileClient.object(OBJECT_AUTHORIZER, config);
        return client;
    }

    //判断文件是否存在
    @Override public boolean doesObjectExist(String bucketName, String key) throws UfileServerException, UfileClientException{
        try {
            logger.trace("UfileServiceImpl.doesObjectExist, bucket:[{}], key:[{}]", bucketName, key);
            ObjectProfile response = this.client.objectProfile(key, bucketName).execute();
        }catch(UfileServerException e){
            //logger.error("UfileServiceImpl.doesObjectExist: [{}]", e.toString());
            return false;   //TODO, 判断异常码
        }
        return true;
    }

    //判断bucket是否存在
    @Override public boolean doesBucketExist(String bucketName){
        try {
            logger.trace("UfileServiceImpl.doesBucketExist, bucket:[{}]", bucketName);
            this.client.objectList(bucketName)
                    .withPrefix("")
                    .withMarker("")
                    .dataLimit(1)
                    .execute();
            logger.trace("UfileServiceImpl.doesBucketExist: true");
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
    @Override public ObjectListBean listObjects(String bucketName, String prefix, String marker)  throws UfileServerException, UfileClientException{
        logger.error("UfileServiceImpl.listObjects, bucket:[{}], prefix:[{}], marker:[{}]", bucketName, prefix, marker);
        return this.client.objectList(bucketName)
                .withPrefix(prefix)
                .withMarker(marker)
                .dataLimit(50)
                .execute();
    }

    //下载文件
    @Override public DownloadStreamBean getObject(String bucketName, String key) throws UfileServerException,UfileClientException {
        logger.error("UfileServiceImpl.getObject, bucket:[{}], key:[{}]", bucketName, key);
        String url = this.client.getDownloadUrlFromPrivateBucket(key, bucketName,30*60)
                .createUrl();
        DownloadStreamBean down_bean = this.client.getStream(url).execute();
        logger.trace("UfileServiceImpl.getObject, bucket:[{}], key:[{}], len:[{}]", bucketName, key, down_bean.getContentLength());
        return down_bean;
    }

    //上传文件
    @Override public void putObject(String bucketName, String key, InputStream input, long blobSize) throws UfileServerException, UfileClientException {
        logger.error("UfileServiceImpl.putObject, bucket:[{}], key:[{}], size:[{}]", bucketName, key, blobSize);

        //String mineType = MimeTypeUtil.getMimeType(new File(key));
        String mimeType = "application/octet-stream";
        if(blobSize<10240000) { //10M以内小文件用put
            this.client.putObject(input, mimeType).nameAs(key).toBucket(bucketName).execute();
            return;
        }

        //否则是大文件，用mput 接口上传
        // 先初始化分片上环请求
        logger.trace("UfileServiceImpl.initMultiUpload");
        MultiUploadInfo upload_info =this.client.initMultiUpload(key, mimeType, bucketName).execute();
        if (upload_info == null)
            throw new UfileServerException("upload init null");

        logger.trace("UfileServiceImpl.multiUpload");
        List<MultiUploadPartState> partStates = multiUpload(input, upload_info);
        // 若上传分片结果列表为空，则失败，需中断上传操作。否则完成上传
        if (partStates == null || partStates.isEmpty()) {
            logger.error("UfileServiceImpl.abortMultiUpload");
            this.client.abortMultiUpload(upload_info).execute();
        }else {
            logger.trace("UfileServiceImpl.finishMultiUpload");
            this.client.finishMultiUpload(upload_info, partStates).execute();
        }
        return ;
    }

    public List<MultiUploadPartState> multiUpload(InputStream is, MultiUploadInfo upload_info) throws UfileServerException, UfileClientException {
        logger.trace("UfileServiceImpl.multiUpload, bucket:[{}], key:[{}]", upload_info.getBucket(), upload_info.getKeyName());

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
                        e.printStackTrace();
                        // 尝试次数+1
                        uploadCount++;
                    }
                }
                if (uploadCount == 3)
                    return null;
            }

            return part_states;
        } catch (IOException e) {
            throw new UfileClientException(e.getMessage());
        }finally {
            FileUtil.close(is);
        }
    }

    //删除文件
    @Override public void deleteObject(String bucketName, String key) throws UfileServerException, UfileClientException {
        logger.error("UfileServiceImpl.deleteObject, bucket:[{}], key:[{}]", bucketName, key);
        this.client.deleteObject(key, bucketName).execute();
    }

    //复制文件，TODO
    @Override public void copyObject(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey) throws UfileServerException, UfileClientException {
        logger.error("UfileServiceImpl.copyObject, src_bucket:[{}], src_key:[{}], dst_bucket:[{}], dst_key:[{}]", sourceBucketName, sourceKey, destinationBucketName, destinationKey);

        String url = this.client.getDownloadUrlFromPrivateBucket(sourceKey, sourceBucketName,30*60)
                .createUrl();

        DownloadStreamBean down_bean;
        try {
            down_bean = this.client.getStream(url).execute();
        }catch(UfileServerException | UfileClientException e){
            logger.error("download exception [{}]", e.getMessage());
            throw e;
        }
        //TODO: make and use copy cmd
        try{
            String mine_type = down_bean.getContentType();
            this.putObject(destinationBucketName, destinationKey, down_bean.getInputStream(), down_bean.getContentLength());
            //this.client.putObject(down_bean.getInputStream(), mine_type).nameAs(destinationKey).toBucket(destinationBucketName).execute();
        }catch(UfileServerException | UfileClientException e){
            logger.error("upload exception [{}]", e.getMessage());
            throw e;
        }

        return;
    }

    @Override public void shutdown() { }
}
