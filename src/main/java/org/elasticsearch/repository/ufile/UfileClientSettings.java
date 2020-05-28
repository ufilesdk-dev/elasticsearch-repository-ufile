package org.elasticsearch.repository.ufile;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import static org.elasticsearch.common.settings.Setting.*;

//TODO: tofix：if use with oss plugin in elasticsearch，will error on Setting Twice
public class UfileClientSettings {
    private static final ByteSizeValue MIN_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.MB);
    private static final ByteSizeValue MAX_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.GB);

    public static final Setting<String> PUBLIC_KEY =
            Setting.simpleString("public_key", Property.NodeScope, Property.Dynamic);
    public static final Setting<String> PRIVATE_KEY = Setting
            .simpleString("private_key", Property.NodeScope, Property.Dynamic);
    public static final Setting<String> ENDPOINT =
            Setting.simpleString("endpoint", Property.NodeScope, Property.Dynamic);

    //    public static final Setting<String> SECURITY_TOKEN = Setting
//            .simpleString("security_token", Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<String> BUCKET =
            simpleString("bucket", Property.NodeScope, Property.Dynamic);
    public static final Setting<String> BASE_PATH =
            simpleString("base_path", Property.NodeScope, Property.Dynamic);
    public static final Setting<Boolean> COMPRESS =
            boolSetting("compress", false, Property.NodeScope, Property.Dynamic);
    public static final Setting<ByteSizeValue> CHUNK_SIZE =
            byteSizeSetting("chunk_size", MAX_CHUNK_SIZE, MIN_CHUNK_SIZE, MAX_CHUNK_SIZE,
                    Property.NodeScope, Property.Dynamic);
    public static final int TRY_DELAY_BASE_TIME = 500;

    public static final int DEFAULT_MAX_TYRTIMES = 3;
}
