package org.elasticsearch.repository.ufile;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Map;


public class UfileBlobContainer extends AbstractBlobContainer {
    private final Logger logger = LogManager.getLogger(UfileBlobContainer.class);
    private final UfileBlobStore blobStore;
    private final String keyPath;

    UfileBlobContainer(BlobPath path, UfileBlobStore blobStore) {
        super(path);
        this.keyPath = path.buildAsString();
        this.blobStore = blobStore;
    }

    /**
     * Tests whether a blob with the given blob name exists in the container.
     *
     * @param blobName The name of the blob whose existence is to be determined.
     * @return {@code true} if a blob exists in the BlobContainer with the given name, and {@code false} otherwise.
     */
    @Override
    public boolean blobExists(String blobName) {
        logger.debug("blobExists({})", blobName);
        try {
            return blobStore.blobExists(buildKey(blobName));
        } catch (Exception e) {
            logger.error("can not access [{}] in bucket {{}}: {}", blobName, blobStore.getBucket(),
                    e.getMessage());
            throw new BlobStoreException("Failed to check if blob [" + blobName + "] exists", e);
        }
    }

    /**
     * Creates a new {@link InputStream} for the given blob name.
     *
     * @param blobName The name of the blob to get an {@link InputStream} for.
     * @return The {@code InputStream} to read the blob.
     * @throws NoSuchFileException if the blob does not exist
     * @throws IOException         if the blob can not be read.
     */
    @Override
    public InputStream readBlob(String blobName) throws IOException {
        logger.debug("readBlob({})", blobName);
        if (!blobExists(blobName)) {
            throw new NoSuchFileException("[" + blobName + "] blob not found");
        }

        return blobStore.readBlob(buildKey(blobName));
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        logger.debug("writeBlob({}, stream, {})", blobName, blobSize);
        blobStore.writeBlob(buildKey(blobName), inputStream, blobSize);
    }

    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        logger.debug("writeBlobAtomic({}, stream, {})", blobName, blobSize);
        writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    /**
     * Deletes a blob with giving name, if the blob exists.  If the blob does not exist, this method throws an IOException.
     *
     * @param blobName The name of the blob to delete.
     * @throws NoSuchFileException if the blob does not exist
     * @throws IOException         if the blob exists but could not be deleted.
     */
    @Override
    public void deleteBlob(String blobName) throws IOException {
        logger.debug("deleteBlob({})", blobName);
        if (!blobExists(blobName)) {
            throw new NoSuchFileException("Blob [" + blobName + "] does not exist");
        }
        deleteBlobIgnoringIfNotExists(blobName);
    }


    @Override
    public void deleteBlobIgnoringIfNotExists(String blobName) throws IOException {
        logger.debug("deleteBlobIgnoringIfNotExists({})", blobName);
        try {
            blobStore.deleteBlob(buildKey(blobName));
        } catch (Exception e) {
            logger.error("can not access [{}] in bucket {{}}: {}", blobName, blobStore.getBucket(),
                    e.getMessage());
            throw new IOException(e);
        }
    }

    /**
     * Lists all blobs in the container.
     *
     * @return A map of all the blobs in the container.  The keys in the map are the names of the blobs and
     * the values are {@link BlobMetaData}, containing basic information about each blob.
     * @throws IOException if there were any failures in reading from the blob container.
     */
    @Override
    public Map<String, BlobMetaData> listBlobs() throws IOException {
        return listBlobsByPrefix(null);
    }

    /**
     * Lists all blobs in the container.
     *
     * @return A map of all the blobs in the container.  The keys in the map are the names of the blobs and
     * the values are {@link BlobMetaData}, containing basic information about each blob.
     * @throws IOException if there were any failures in reading from the blob container.
     */
    @Override
    public Map<String, BlobMetaData> listBlobsByPrefix(String blobNamePrefix)
            throws IOException {
        logger.debug("listBlobsByPrefix({})", blobNamePrefix);
        try {
            return blobStore.listBlobsByPrefix(keyPath, blobNamePrefix);
        } catch (IOException e) {
            logger.error("can not access [{}] in bucket {{}}: {}", blobNamePrefix,
                    blobStore.getBucket(), e.getMessage());
            throw new IOException(e);
        }
    }

    protected String buildKey(String blobName) {
        return keyPath + (blobName == null ? StringUtils.EMPTY : blobName);
    }
}
