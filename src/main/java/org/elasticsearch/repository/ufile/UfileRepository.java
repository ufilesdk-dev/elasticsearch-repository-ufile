package org.elasticsearch.repository.ufile;

import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.File;


public class UfileRepository extends BlobStoreRepository {
    static final String TYPE = "ufile";

    private final UfileBlobStore blobStore;
    private final BlobPath basePath;
    private final boolean compress;
    private final ByteSizeValue chunkSize;

    public UfileRepository(RepositoryMetaData metadata, Environment env,
                           NamedXContentRegistry namedXContentRegistry, UfileService ufileService) {
        super(metadata, env.settings(), namedXContentRegistry);

        String bucket = UfileClientSettings.BUCKET.get(metadata.settings());

        String basePath = UfileClientSettings.BASE_PATH.get(metadata.settings());
        if (Strings.hasLength(basePath)) {
            BlobPath path = new BlobPath();
            for (String elem : basePath.split(File.separator)) {
                path = path.add(elem);
            }
            this.basePath = path;
        } else {
            this.basePath = BlobPath.cleanPath();
        }
        this.compress = UfileClientSettings.COMPRESS.get(metadata.settings());
        this.chunkSize = UfileClientSettings.CHUNK_SIZE.get(metadata.settings());
        logger.debug("using bucket [{}], base_path [{}], chunk_size [{}], compress [{}]", bucket,
                basePath, chunkSize, compress);
        blobStore = new UfileBlobStore(env.settings(), bucket, ufileService);
    }

    @Override
    protected BlobStore blobStore() {
        return this.blobStore;
    }

    @Override
    protected BlobPath basePath() {
        return this.basePath;
    }

    @Override
    protected boolean isCompress() {
        return compress;
    }


    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }

}
