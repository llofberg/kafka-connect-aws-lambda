package com.tm.kafka.connect.aws.lambda;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.model.InvocationType;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;


public class AwsLambdaSinkConnectorConfig extends AbstractConfig {

  public static final String REGION_CONFIG = "aws.region";
  public static final String REGION_DOC_CONFIG = "The AWS region.";
  public static final String REGION_DISPLAY_CONFIG = "AWS region";

  public static final String CREDENTIALS_PROVIDER_CLASS_CONFIG = "aws.credentials.provider.class";
  public static final Class<? extends AWSCredentialsProvider> CREDENTIALS_PROVIDER_CLASS_DEFAULT =
    DefaultAWSCredentialsProviderChain.class;
  public static final String CREDENTIALS_PROVIDER_DOC_CONFIG =
    "Credentials provider or provider chain to use for authentication to AWS. By default "
      + "the connector uses 'DefaultAWSCredentialsProviderChain'.";
  public static final String CREDENTIALS_PROVIDER_DISPLAY_CONFIG = "AWS Credentials Provider Class";

  public static final String FUNCTION_NAME_CONFIG = "aws.function.name";
  public static final String FUNCTION_NAME_DOC = "The AWS Lambda function name.";
  public static final String FUNCTION_NAME_DISPLAY = "AWS Lambda function Name";

  public static final String RETRY_BACKOFF_CONFIG = "aws.function.retry.backoff.ms";
  public static final String RETRY_BACKOFF_DOC =
    "The retry backoff in milliseconds. This config is used to notify Kafka connect to retry "
      + "delivering a message batch or performing recovery in case of transient exceptions.";
  public static final long RETRY_BACKOFF_DEFAULT = 5000L;
  public static final String RETRY_BACKOFF_DISPLAY = "Retry Backoff (ms)";

  private static final String INVOCATION_TYPE_CONFIG = "aws.lambda.invocation.type";
  public static final String INVOCATION_TYPE_DEFAULT = "RequestResponse";
  public static final String INVOCATION_TYPE_DOC_CONFIG = "AWS Lambda function invocation type.";
  public static final String INVOCATION_TYPE_DISPLAY_CONFIG = "Invocation type";

  public AwsLambdaSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
  }

  public AwsLambdaSinkConnectorConfig(Map<String, String> parsedConfig) {
    this(conf(), parsedConfig);
  }

  public static ConfigDef conf() {
    String group = "AWS";
    int orderInGroup = 0;
    return new ConfigDef()
      .define(REGION_CONFIG,
        Type.STRING,
        NO_DEFAULT_VALUE,
        new RegionValidator(),
        Importance.HIGH,
        REGION_DOC_CONFIG,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        REGION_DISPLAY_CONFIG,
        new RegionRecommender())

      .define(CREDENTIALS_PROVIDER_CLASS_CONFIG,
        Type.CLASS,
        CREDENTIALS_PROVIDER_CLASS_DEFAULT,
        new CredentialsProviderValidator(),
        Importance.HIGH,
        CREDENTIALS_PROVIDER_DOC_CONFIG,
        group,
        ++orderInGroup,
        ConfigDef.Width.MEDIUM,
        CREDENTIALS_PROVIDER_DISPLAY_CONFIG)

      .define(FUNCTION_NAME_CONFIG,
        Type.STRING,
        NO_DEFAULT_VALUE,
        Importance.HIGH,
        FUNCTION_NAME_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        FUNCTION_NAME_DISPLAY)

      .define(RETRY_BACKOFF_CONFIG,
        Type.LONG,
        RETRY_BACKOFF_DEFAULT,
        Importance.LOW,
        RETRY_BACKOFF_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        RETRY_BACKOFF_DISPLAY)

      .define(INVOCATION_TYPE_CONFIG,
        Type.STRING,
        INVOCATION_TYPE_DEFAULT,
        new InvocationTypeValidator(),
        Importance.LOW,
        INVOCATION_TYPE_DOC_CONFIG,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        INVOCATION_TYPE_DISPLAY_CONFIG,
        new InvocationTypeRecommender())
      ;
  }

  public String getAwsRegion() {
    return this.getString(REGION_CONFIG);
  }

  @SuppressWarnings("unchecked")
  public AWSCredentialsProvider getAwsCredentialsProviderClass() {
    try {
      return ((Class<? extends AWSCredentialsProvider>)
        getClass(AwsLambdaSinkConnectorConfig.CREDENTIALS_PROVIDER_CLASS_CONFIG)).getDeclaredConstructor().newInstance();
    } catch (IllegalAccessException | InstantiationException | InvocationTargetException | NoSuchMethodException e) {
      throw new ConnectException("Invalid class for: " + AwsLambdaSinkConnectorConfig.CREDENTIALS_PROVIDER_CLASS_CONFIG, e);
    }
  }

  public String getAwsFunctionName() {
    return this.getString(FUNCTION_NAME_CONFIG);
  }

  public Long getAwsFunctionRetryBackoff() {
    return this.getLong(RETRY_BACKOFF_CONFIG);
  }

  public InvocationType getAwsLambdaInvocationType() {
    return InvocationType .fromValue(this.getString(INVOCATION_TYPE_CONFIG));
  }

  private static class RegionRecommender implements ConfigDef.Recommender {
    @Override
    public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
      return new ArrayList<Object>(RegionUtils.getRegions());
    }

    @Override
    public boolean visible(String name, Map<String, Object> connectorConfigs) {
      return true;
    }
  }

  private static class RegionValidator implements ConfigDef.Validator {
    @Override
    public void ensureValid(String name, Object region) {
      String regionStr = ((String) region).toLowerCase().trim();
      if (RegionUtils.getRegion(regionStr) == null) {
        throw new ConfigException(name, region, "Value must be one of: " + Utils.join(RegionUtils.getRegions(), ", "));
      }
    }

    @Override
    public String toString() {
      return "[" + Utils.join(RegionUtils.getRegions(), ", ") + "]";
    }
  }

  private static class CredentialsProviderValidator implements ConfigDef.Validator {
    @Override
    public void ensureValid(String name, Object provider) {
      if (provider != null && provider instanceof Class
        && AWSCredentialsProvider.class.isAssignableFrom((Class<?>) provider)) {
        return;
      }
      throw new ConfigException(name, provider, "Class must extend: " + AWSCredentialsProvider.class);
    }

    @Override
    public String toString() {
      return "Any class implementing: " + AWSCredentialsProvider.class;
    }
  }

  private static class InvocationTypeRecommender implements ConfigDef.Recommender {
    @Override
    public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
      return Arrays.<Object>asList(InvocationType.values());
    }

    @Override
    public boolean visible(String name, Map<String, Object> connectorConfigs) {
      return true;
    }
  }

  private static class InvocationTypeValidator implements ConfigDef.Validator {
    @Override
    public void ensureValid(String name, Object invocationType) {
      try {
        InvocationType.fromValue(((String) invocationType).trim());
      } catch (Exception e) {
        throw new ConfigException(name, invocationType, "Value must be one of: " +
          Utils.join(InvocationType.values(), ", "));
      }
    }

    @Override
    public String toString() {
      return "[" + Utils.join(InvocationType.values(), ", ") + "]";
    }
  }

  public static ConfigDef getConfig() {
    Map<String, ConfigDef.ConfigKey> everything = new HashMap<>(conf().configKeys());
    ConfigDef visible = new ConfigDef();
    for (ConfigDef.ConfigKey key : everything.values()) {
      visible.define(key);
    }
    return visible;
  }

  public static void main(String[] args) {
    System.out.println(VersionUtil.getVersion());
    System.out.println(getConfig().toEnrichedRst());
  }

}
