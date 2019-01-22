package org.elasticsearch.plugin.repository.ufile;

import cn.ucloud.ufile.util.JLog;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.ucloud.ufile.service.UfileClientSettings;
import org.elasticsearch.ucloud.ufile.service.UfileService;
import org.elasticsearch.ucloud.ufile.service.UfileServiceImpl;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repository.ufile.UfileRepository;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A plugin to add a repository type that writes to and from Ufile.
 * from yangkongshi on 2017/11/24.
 */
public class UfileRepositoryPlugin extends Plugin implements RepositoryPlugin {

    static {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            return null;
        });
        //JLog.SHOW_TEST=true;
        //JLog.SHOW_DEBUG=true;
        //JLog.init("/data/alog/", 100, true);
    }

    protected UfileService createStorageService(Settings settings, RepositoryMetaData metadata) {
        return new UfileServiceImpl(settings, metadata);
    }

    @Override public Map<String, Repository.Factory> getRepositories(Environment env,
        NamedXContentRegistry namedXContentRegistry) {
        return Collections.singletonMap(UfileRepository.TYPE,
            (metadata) -> new UfileRepository(metadata, env, namedXContentRegistry,
                createStorageService(env.settings(), metadata)));
    }

    @Override public List<Setting<?>> getSettings() {
        return Arrays.asList(UfileClientSettings.ACCESS_KEY_ID, UfileClientSettings.SECRET_ACCESS_KEY,
            UfileClientSettings.ENDPOINT, UfileClientSettings.BUCKET, UfileClientSettings.SECURITY_TOKEN,
            UfileClientSettings.BASE_PATH, UfileClientSettings.COMPRESS, UfileClientSettings.CHUNK_SIZE);

    }
}
