package org.elasticsearch.repository.ufile;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class UfileRepositoryPlugin extends Plugin implements RepositoryPlugin {

    static {
        SpecialPermission.check();
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> null);
    }

    protected UfileService createStorageService(Settings settings, RepositoryMetaData metadata) {
        return new UfileServiceImpl(settings, metadata);
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(Environment env, NamedXContentRegistry namedXContentRegistry) {
        return Collections.singletonMap(UfileRepository.TYPE,
                (metadata) -> new UfileRepository(metadata, env, namedXContentRegistry, createStorageService(env.settings(), metadata)));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
                UfileClientSettings.PUBLIC_KEY,
                UfileClientSettings.PRIVATE_KEY,
                UfileClientSettings.ENDPOINT,
                UfileClientSettings.BUCKET,
                UfileClientSettings.BASE_PATH,
                UfileClientSettings.COMPRESS,
                UfileClientSettings.CHUNK_SIZE);

    }
}
