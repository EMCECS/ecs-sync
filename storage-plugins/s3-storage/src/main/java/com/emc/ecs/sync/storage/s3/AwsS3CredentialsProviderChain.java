package com.emc.ecs.sync.storage.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import java.util.ArrayList;
import java.util.List;

public class AwsS3CredentialsProviderChain extends AWSCredentialsProviderChain {
    private static final EnvironmentVariableCredentialsProvider ENV_PROVIDER_INSTANCE = new EnvironmentVariableCredentialsProvider();
    private static final SystemPropertiesCredentialsProvider SYS_PROVIDER_INSTANCE = new SystemPropertiesCredentialsProvider();
    private static final ProfileCredentialsProvider PROFILE_CREDENTIALS_PROVIDER = new ProfileCredentialsProvider();
    private static final EC2ContainerCredentialsProviderWrapper EC2_PROVIDER_WRAPPER = new EC2ContainerCredentialsProviderWrapper();

    private AwsS3CredentialsProviderChain(List<? extends AWSCredentialsProvider> credentialsProviders) {
        super(credentialsProviders);
    }

    public static AwsS3CredentialsProviderChain.Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final ArrayList<AWSCredentialsProvider> credentialProviders = new ArrayList<>();

        Builder() {
        }

        public AwsS3CredentialsProviderChain build() {
            return new AwsS3CredentialsProviderChain(credentialProviders);
        }

        public Builder addProfileCredentialsProvider(String profile) {
            credentialProviders.add(new ProfileCredentialsProvider(profile));
            return this;
        }

        public Builder addDefaultProviders() {
            credentialProviders.add(ENV_PROVIDER_INSTANCE);
            credentialProviders.add(SYS_PROVIDER_INSTANCE);
            credentialProviders.add(PROFILE_CREDENTIALS_PROVIDER);
            credentialProviders.add(EC2_PROVIDER_WRAPPER);
            return this;
        }

        public Builder addCredentials(AWSCredentials credentials) {
            return addCredentialsProvider(new AWSStaticCredentialsProvider(credentials));
        }

        public Builder addCredentialsProvider(AWSCredentialsProvider provider) {
            credentialProviders.add(provider);
            return this;
        }
    }
}
