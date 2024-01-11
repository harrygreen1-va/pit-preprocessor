package pit.etl.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pit.TableMeta;
import pit.UnrecoverableException;
import pit.etl.batchlog.BatchTracker;
import pit.etl.dbloader.JobInfo;
import pit.util.FilePathUtils;
import pit.util.Utils;

import java.io.File;
import java.util.*;

@Slf4j
public class Config {

    @SuppressWarnings("FieldCanBeLocal")
    private static final Logger logger = LoggerFactory.getLogger(Config.class);
    public final static String DRY_RUN_PROP_NAME = "is_current.dry.run";
    private final Configuration config;
    private final File configFile;

    public static Config load() {
        return load(null);
    }

    public static Config load(String env) {
        if (env == null) {
            env = System.getProperty(ENV_SYSTEM_PROP);
            if (env == null) {
                logger.info("The environment was not provided, will attempt to deduce from the host name");
                env = deduceEnv();
            }
        }

        String fileName = env + ".properties";

        File configFile = new File(FilePathUtils.getAbsoluteFilePath(fileName));
        log.info("Environment config: {}", fileName);
        return new Config(env, configFile);
    }

    public final static String ENV_SYSTEM_PROP = "pit.etl.env";

    private static String deduceEnv() {
        String env;

        String hostName = Utils.getHostname();

        if (hostName == null) {
            throw new IllegalStateException("Unable to determine environment from the host name, host name is null");
        }

        if (hostName.contains("2")) {
            env = "prod";
        } else if (hostName.contains("4")) {
            env = "preprod";
        } else {
            throw new IllegalStateException(
                    "Unable to determine environment based on the host " + hostName);
        }

        logger.info("Deduced environment: {}", env);
        return env;
    }


    private String env;

    public Config(String env, File configFile) {
        this(configFile);
        this.env = env;
    }

    public Config(File configFile) {
        this.configFile = configFile;
        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
                new FileBasedConfigurationBuilder<>(PropertiesConfiguration.class);

        builder.configure(params.properties()
                .setFile(configFile)
                .setListDelimiterHandler(new DefaultListDelimiterHandler(','))
                .setThrowExceptionOnMissing(true));
        try {
            config = builder.getConfiguration();
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }

        logger.debug("Loaded configuration properties from " + configFile.getPath());
    }

    public String env() {
        return env;
    }

    /**
     * Get the property fist from system prop and then from the config file
     *
     * @param propName prop name
     * @return prop value or exception if does not exist
     */
    public String get(String propName) {
        String val = System.getProperty(propName);
        if (val == null)
            val = config.getString(propName);
        return val;
    }

    public String get(String propName, String defaultVal) {
        if (contains(propName)) {
            return get(propName);
        }
        return defaultVal;
    }

    public boolean contains(String propName) {
        return config.containsKey(propName);
    }


    public int getInt(String propName) {
        return config.getInt(propName);
    }

    public boolean isTrue(String propName) {
        return config.containsKey(propName) && config.getBoolean(propName);
    }

    public List<String> getStringList(String name) {
        if (!config.containsKey(name))
            return new ArrayList<>();

        return Arrays.asList(config.getStringArray(name));
    }

    public Set<String> getStringSet(String name) {
        if (!config.containsKey(name))
            return new HashSet<>();

        return new LinkedHashSet<>(Arrays.asList(config.getStringArray(name)));
    }

    public List<String> getRequiredStringList(String name) {
        var list = getStringList(name);
        if (list.isEmpty()) {
            throw new UnrecoverableException("Required property %s is not defined in %s", name, configFile.getName());
        }

        return list;
    }

    public Map<String, String> getProps(String prefix) {
        Map<String, String> props = new HashMap<>();
        Iterator<String> keys = config.getKeys(prefix);
        while (keys.hasNext()) {
            String key = keys.next();
            String keyNoPrefix = StringUtils.removeStart(key, prefix + ".");
            props.put(keyNoPrefix, config.getString(key));
        }

        return props;
    }

    public Map<String, Integer> getIntProps(String prefix) {
        Map<String, Integer> props = new HashMap<>();
        Iterator<String> keys = config.getKeys(prefix);
        while (keys.hasNext()) {
            String key = keys.next();
            String keyNoPrefix = StringUtils.removeStart(key, prefix + ".");
            props.put(keyNoPrefix, config.getInt(key));
        }

        return props;
    }

    public String getConcatFileLocation() {
        return get("concat.file.location");
    }


    @SuppressWarnings("unused")
    public void printKeys() {
        System.out.println("Keys:");
        Iterator<String> keys = config.getKeys();
        while (keys.hasNext())
            System.out.println(keys.next());

    }


    public static JobInfo jobInfoFromConfig(String jobConfigName, BatchTracker batch) {
        String fileName = "jobconfig/" + jobConfigName + ".properties";

        File configFile = new File(FilePathUtils.getAbsoluteFilePath(fileName));
        Config config = new Config(configFile);

        return JobInfo.fromConfig(config, batch);

    }

    public boolean isDryRun() {
        return isTrue(DRY_RUN_PROP_NAME);
    }

    public static TableMeta tableMetaFromConfig(String tableMetaName) {
        String fileName = "tablemeta/" + tableMetaName + ".properties";

        File configFile = new File(FilePathUtils.getAbsoluteFilePath(fileName));

        Config config = new Config(configFile);

        return TableMeta.fromConfig(config);
    }


    public final static String DISABLE_LOADING_SYSPROP = "disable_loading";

    public static boolean isDataLoadingEnabled() {
        return !BooleanUtils.toBoolean(System.getProperty(DISABLE_LOADING_SYSPROP));
    }

}