package com.platformlunar.digdag.plugin.ecs;

import com.google.common.io.BaseEncoding; // TODO explicit dependency
import com.google.common.base.Optional; // TODO explicit dependency
import com.google.common.base.Supplier;

import io.digdag.client.config.ConfigException;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.SecretProvider;
import io.digdag.util.BaseOperator;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

import java.util.concurrent.ThreadLocalRandom;

public abstract class BaseEcsOperator extends BaseOperator {
    public BaseEcsOperator(OperatorContext context) {
        super(context);
    }

    // TODO: copied from https://github.com/treasure-data/digdag/blob/b68b4dd2fd9a16b3751decf074c5bdc4b1d2c86b/digdag-standards/src/main/java/io/digdag/standards/operator/aws/EmrOperatorFactory.java#L274-L309
    // should likely be contributed upstream
    protected AWSCredentials credentials(String tag)
    {
        SecretProvider awsSecrets = context.getSecrets().getSecrets("aws");
        SecretProvider ecsSecrets = awsSecrets.getSecrets("ecs");

        String accessKeyId = ecsSecrets.getSecretOptional("access_key_id")
            .or(() -> awsSecrets.getSecret("access_key_id"));

        String secretAccessKey = ecsSecrets.getSecretOptional("secret_access_key")
            .or(() -> awsSecrets.getSecret("secret_access_key"));

        AWSCredentials credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);

        return credentials;
    }

    protected static String randomTag()
    {
        byte[] bytes = new byte[8];
        ThreadLocalRandom.current().nextBytes(bytes);
        return BaseEncoding.base32().omitPadding().encode(bytes);
    }

    @SafeVarargs
    protected static <T> Optional<T> first(Supplier<Optional<T>>... suppliers) {
        for (Supplier<Optional<T>> supplier : suppliers) {
            Optional<T> optional = supplier.get();
            if (optional.isPresent()) {
                return optional;
            }
        }
        return Optional.absent();
    }
}
