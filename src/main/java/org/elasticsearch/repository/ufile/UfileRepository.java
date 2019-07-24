package org.elasticsearch.repository.ufile;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.File;


public class UfileRepository extends BlobStoreRepository {
    private static final Logger logger = LogManager.getLogger(UfileRepository.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    public static final String TYPE = "ufile";
    private final BlobPath basePath;
    private final boolean compress;
    private final ByteSizeValue chunkSize;
    private final String bucket;
    private final UfileService service;
    private final Settings settings;


    public UfileRepository(RepositoryMetaData metadata, Environment env,
                           NamedXContentRegistry namedXContentRegistry, UfileService service) {
        super(metadata, env.settings(), namedXContentRegistry);
        this.settings = env.settings();
        this.service = service;
        this.bucket = getSetting(UfileClientSettings.BUCKET, metadata);
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
        this.compress = getSetting(UfileClientSettings.COMPRESS, metadata);
        this.chunkSize = getSetting(UfileClientSettings.CHUNK_SIZE, metadata);
        logger.debug("using bucket [{}], base_path [{}], chunk_size [{}], compress [{}]", this.bucket,
                basePath, chunkSize, compress);
    }

    @Override
    protected UfileBlobStore createBlobStore() {
        return new UfileBlobStore(settings, bucket, service);
    }

    @Override
    protected BlobStore blobStore() {
        return super.blobStore();
    }

    @Override
    protected BlobStore getBlobStore() {
        return super.getBlobStore();
    }

    @Override
    protected BlobPath basePath() {
        return basePath;
    }

    @Override
    protected boolean isCompress() {
        return compress;
    }

    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }

    public static <T> T getSetting(Setting<T> setting, RepositoryMetaData metadata) {
        T value = setting.get(metadata.settings());
        if (value == null) {
            throw new RepositoryException(metadata.name(),
                    "Setting [" + setting.getKey() + "] is not defined for repository");
        }
        if ((value instanceof String) && (Strings.hasText((String) value)) == false) {
            throw new RepositoryException(metadata.name(),
                    "Setting [" + setting.getKey() + "] is empty for repository");
        }
        return value;
    }
}
