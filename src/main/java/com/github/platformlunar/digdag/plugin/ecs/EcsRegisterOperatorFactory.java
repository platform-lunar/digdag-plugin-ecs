package com.github.platformlunar.digdag.plugin.ecs;

import com.google.common.base.Optional;

import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigKey;
import io.digdag.client.config.ConfigException;
import io.digdag.core.Environment;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TemplateEngine;
import io.digdag.spi.TaskResult;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionException;
import io.digdag.standards.operator.state.TaskState;
import io.digdag.standards.operator.DurationInterval;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ecs.AmazonECSClient;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.amazonaws.services.ecs.model.ContainerDefinition;
import com.amazonaws.services.ecs.model.HostEntry;
import com.amazonaws.services.ecs.model.HostVolumeProperties;
import com.amazonaws.services.ecs.model.KeyValuePair;
import com.amazonaws.services.ecs.model.LogConfiguration;
import com.amazonaws.services.ecs.model.MountPoint;
import com.amazonaws.services.ecs.model.PortMapping;
import com.amazonaws.services.ecs.model.RegisterTaskDefinitionRequest;
import com.amazonaws.services.ecs.model.DescribeTaskDefinitionRequest;
import com.amazonaws.services.ecs.model.DescribeTaskDefinitionResult;
import com.amazonaws.services.ecs.model.ListTaskDefinitionsRequest;
import com.amazonaws.services.ecs.model.ListTaskDefinitionsResult;
import com.amazonaws.services.ecs.model.TaskDefinitionPlacementConstraint;
import com.amazonaws.services.ecs.model.TaskDefinition;
import com.amazonaws.services.ecs.model.TaskOverride;
import com.amazonaws.services.ecs.model.RegisterTaskDefinitionResult;
import com.amazonaws.services.ecs.model.Ulimit;
import com.amazonaws.services.ecs.model.Volume;
import com.amazonaws.services.ecs.model.VolumeFrom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Deque;
import java.util.Map;
import java.util.List;
import java.util.LinkedList;

import static io.digdag.standards.operator.state.PollingRetryExecutor.pollingRetryExecutor;

import static java.util.stream.Collectors.toList;

public class EcsRegisterOperatorFactory implements OperatorFactory
{

    private final TemplateEngine templateEngine;
    private final Map<String, String> environment;

    private static final String STATE_START = "start";

    private static Logger logger = LoggerFactory.getLogger(EcsOperator.class);

    public EcsRegisterOperatorFactory(TemplateEngine templateEngine, @Environment Map<String, String> environment)
    {
        this.templateEngine = templateEngine;
        this.environment = environment;
    }

    public String getType()
    {
        return "ecs_register";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new EcsOperator(context);
    }


    private class EcsOperator extends BaseEcsOperator
    {
        private final TaskState state;

        public EcsOperator(OperatorContext context)
        {
            super(context);
            this.state = TaskState.of(request);
        }

        @Override
        public TaskResult runTask()
        {
            SecretProvider secrets = context.getSecrets().getSecrets("ecs");

            Config config = request.getConfig().mergeDefault(
                request.getConfig().getNestedOrGetEmpty("ecs"));

            String tag = state.constant("tag", String.class, EcsOperator::randomTag);

            AWSCredentials credentials = credentials(tag);

            ClientConfiguration ecsClientConfiguration = new ClientConfiguration();

            SecretProvider awsSecrets = context.getSecrets().getSecrets("aws");
            SecretProvider ecsSecrets = awsSecrets.getSecrets("ecs");

            Optional<String> ecsRegionName = first(
                () -> ecsSecrets.getSecretOptional("region"),
                () -> awsSecrets.getSecretOptional("region"),
                () -> config.getOptional("ecs.region", String.class));

            Optional<String> ecsEndpoint = first(
                () -> ecsSecrets.getSecretOptional("endpoint"),
                () -> config.getOptional("ecs.endpoint", String.class),
                () -> ecsRegionName.transform(regionName -> "ecs." + regionName + ".amazonaws.com"));

            AmazonECSClientBuilder clientBuilder = AmazonECSClient.builder()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withClientConfiguration(ecsClientConfiguration);

            if (ecsEndpoint.isPresent() && ecsRegionName.isPresent()) {
                clientBuilder = clientBuilder.withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(
                        ecsEndpoint.get(), ecsRegionName.get()
                    )
                );
            } else if (ecsRegionName.isPresent()) {
                Regions region;
                try {
                    region = Regions.fromName(ecsRegionName.get());

                    clientBuilder = clientBuilder.withRegion(Region.getRegion(region).getName());
                }
                catch (IllegalArgumentException e) {
                    throw new ConfigException("Illegal AWS region: " + ecsRegionName.get());
                }
            }

            AmazonECSClient client = (AmazonECSClient) clientBuilder.build();

            String familyName = config.get("family", String.class);

            TaskDefinition taskDefinition = pollingRetryExecutor(state, STATE_START)
                .withErrorMessage("ECS task definition %s failed to register", familyName)
                .runOnce(TaskDefinition.class, (TaskState state) -> {
                        logger.info("Registering ECS task definition family {}", familyName);

                        try {
                            return getOrCreateTaskDefinition(client, familyName, config);
                        } catch (com.amazonaws.AmazonServiceException ex) {
                            throw new TaskExecutionException(ex);
                        }
                    }
                );

            return buildResult(taskDefinition);
        }

        private TaskResult buildResult(TaskDefinition taskDefinition) {
            ConfigFactory cf = request.getConfig().getFactory();
            Config result = cf.create();
            Config ecs = result.getNestedOrSetEmpty("ecs");

            ecs.set("last_task_family", taskDefinition.getFamily() + ":" + taskDefinition.getRevision());

            return TaskResult.defaultBuilder(request)
                .storeParams(result)
                .addResetStoreParams(ConfigKey.of("ecs", "last_task_family"))
                .build();
        }

        private TaskDefinition getOrCreateTaskDefinition(AmazonECSClient client,
                                                         String familyName,
                                                         Config config) {
            List<ContainerDefinition> containerDefinitions = config.getListOrEmpty("container_definitions", Config.class)
                .stream()
                .map(this::buildContainerDefinition)
                .collect(toList());

            List<Volume> volumes = config.getListOrEmpty("volumes", Config.class)
                .stream()
                .map(this::buildVolume)
                .collect(toList());

            List<TaskDefinitionPlacementConstraint> placementConstraints = config.getListOrEmpty("placement_constraints", Config.class)
                .stream()
                .map(this::buildPlacementConstraint)
                .collect(toList());

            String taskRoleArn = config.get("task_role_arn", String.class, null);
            String networkMode = config.get("network_mode", String.class, null);

            TaskDefinition currentTaskDefinition = getTaskDefinition(client,
                                                                     containerDefinitions,
                                                                     volumes,
                                                                     placementConstraints,
                                                                     familyName,
                                                                     taskRoleArn,
                                                                     networkMode);

            if (currentTaskDefinition == null) {
                RegisterTaskDefinitionRequest registerDefinitionRequest = new RegisterTaskDefinitionRequest()
                    .withFamily(familyName)
                    .withTaskRoleArn(taskRoleArn)
                    .withNetworkMode(networkMode)
                    .withPlacementConstraints(placementConstraints)
                    .withVolumes(volumes)
                    .withContainerDefinitions(containerDefinitions);

                RegisterTaskDefinitionResult result = client.registerTaskDefinition(registerDefinitionRequest);

                return result.getTaskDefinition();
            }

            return currentTaskDefinition;
        }

        private TaskDefinition getTaskDefinition(AmazonECSClient client,
                                                 List<ContainerDefinition> containerDefinitions,
                                                 List<Volume> volumes,
                                                 List<TaskDefinitionPlacementConstraint> placementConstraints,
                                                 String familyName,
                                                 String taskRoleArn,
                                                 String networkMode) {
            String lastToken = null;
            Deque<String> taskDefinitions = new LinkedList<String>();

            do {
                ListTaskDefinitionsRequest listTaskDefinitionRequest = new ListTaskDefinitionsRequest()
                    .withFamilyPrefix(familyName)
                    .withMaxResults(100)
                    .withNextToken(lastToken);
                ListTaskDefinitionsResult listTaskDefinitions = client.listTaskDefinitions(listTaskDefinitionRequest);
                taskDefinitions.addAll(listTaskDefinitions.getTaskDefinitionArns());
                lastToken = listTaskDefinitions.getNextToken();
            } while(lastToken != null);

            if (taskDefinitions.size() > 0) {
                DescribeTaskDefinitionRequest describeTaskDefinitionRequest = new DescribeTaskDefinitionRequest()
                    .withTaskDefinition(taskDefinitions.getLast());

                DescribeTaskDefinitionResult describeTaskDefinition = describeTaskDefinition = client.describeTaskDefinition(describeTaskDefinitionRequest);

                TaskDefinition taskDefinition = describeTaskDefinition.getTaskDefinition();

                if (containerDefinitions.equals(taskDefinition.getContainerDefinitions()) &&
                    volumes.equals(taskDefinition.getVolumes()) &&
                    placementConstraints.equals(taskDefinition.getPlacementConstraints()) &&
                    taskRoleArn == taskDefinition.getTaskRoleArn() &&
                    networkMode == taskDefinition.getNetworkMode()) {

                    return taskDefinition;
                }
            }

            return null;
        }

        private Volume buildVolume(Config volumeConfig)
        {
            Config config = request.getConfig().getFactory().create(volumeConfig);
            String name = config.get("name", String.class);

            HostVolumeProperties host = new HostVolumeProperties()
                .withSourcePath(config.get("host_volume_properties.source_path", String.class));

            Volume volume = new Volume()
                .withName(name)
                .withHost(host);

            return volume;
        }

        private TaskDefinitionPlacementConstraint buildPlacementConstraint(Config placementConstraintConfig)
        {
            Config config = request.getConfig().getFactory().create(placementConstraintConfig);

            TaskDefinitionPlacementConstraint placementConstraint = new TaskDefinitionPlacementConstraint()
                .withExpression(config.get("expression", String.class))
                .withType(config.get("type", String.class));

            return placementConstraint;
        }

        private ContainerDefinition buildContainerDefinition(Config containerDefinitionConfig)
        {
            Config config = request.getConfig().getFactory().create(containerDefinitionConfig);

            List<KeyValuePair> kvPairs = config.getListOrEmpty("environment", Config.class)
                .stream()
                .map(this::buildKvPair)
                .collect(toList());

            List<HostEntry> extraHosts = config.getListOrEmpty("extra_hosts", Config.class)
                .stream()
                .map(this::buildHostEntry)
                .collect(toList());

            LogConfiguration logConfiguration = buildLogConfiguration(config.parseNestedOrGetEmpty("log_configuration"));

            List<MountPoint> mountPoints = config.getListOrEmpty("mount_points", Config.class)
                .stream()
                .map(this::buildMountPoint)
                .collect(toList());

            List<PortMapping> portMappings = config.getListOrEmpty("port_mappings", Config.class)
                .stream()
                .map(this::buildPortMapping)
                .collect(toList());

            List<Ulimit> ulimits = config.getListOrEmpty("ulimits", Config.class)
                .stream()
                .map(this::buildUlimit)
                .collect(toList());

            List<VolumeFrom> volumesFrom = config.getListOrEmpty("volumes_from", Config.class)
                .stream()
                .map(this::buildVolumeFrom)
                .collect(toList());

            ContainerDefinition containerDefinition = new ContainerDefinition()
                .withCommand(config.getListOrEmpty("command", String.class))
                .withCpu(config.get("cpu", Integer.class, null))
                .withDisableNetworking(config.get("disabled_networking", Boolean.class, null))
                .withDnsSearchDomains(config.getListOrEmpty("dns_search_domains", String.class))
                .withDnsServers(config.getListOrEmpty("dns_servers", String.class))
                .withDockerSecurityOptions(config.getListOrEmpty("docker_security_options", String.class))
                .withEntryPoint(config.getListOrEmpty("entry_point", String.class))
                .withEnvironment(kvPairs)
                .withEssential(config.get("essential", Boolean.class, null))
                .withExtraHosts(extraHosts)
                .withHostname(config.get("hostname", String.class, null))
                .withImage(config.get("image", String.class))
                .withLinks(config.getListOrEmpty("links", String.class))
                .withLogConfiguration(logConfiguration)
                .withMemory(config.get("memory", Integer.class, null))
                .withMemoryReservation(config.get("memory_reservation", Integer.class, null))
                .withMountPoints(mountPoints)
                .withName(config.get("name", String.class))
                .withPortMappings(portMappings)
                .withPrivileged(config.get("privileged", Boolean.class, null))
                .withReadonlyRootFilesystem(config.get("readonly_root_filesystem", Boolean.class, null))
                .withUlimits(ulimits)
                .withUser(config.get("user", String.class, null))
                .withVolumesFrom(volumesFrom)
                .withWorkingDirectory(config.get("working_directory", String.class, null));

            Map<String, String> dockerLabels = config.getMapOrEmpty("docker_labels", String.class, String.class);

            if (!dockerLabels.isEmpty()) {
                containerDefinition.withDockerLabels(dockerLabels);
            }

            return containerDefinition;
        }

        private KeyValuePair buildKvPair(Config kvPairConfig) {
            Config config = request.getConfig().getFactory().create(kvPairConfig);

            KeyValuePair kvPair = new KeyValuePair()
                .withName(config.get("name", String.class))
                .withValue(config.get("value", String.class));

            return kvPair;
        }

        private HostEntry buildHostEntry(Config extraHostConfig) {
            Config config = request.getConfig().getFactory().create(extraHostConfig);

            HostEntry hostEntry = new HostEntry()
                .withHostname(config.get("hostname", String.class))
                .withIpAddress(config.get("ip_address", String.class));

            return hostEntry;
        }

        private Ulimit buildUlimit(Config ulimitConfig) {
            Config config = request.getConfig().getFactory().create(ulimitConfig);

            Ulimit ulimit = new Ulimit()
                .withName(config.get("name", String.class))
                .withSoftLimit(config.get("soft_limit", Integer.class))
                .withHardLimit(config.get("hard_limit", Integer.class));

            return ulimit;
        }

        private VolumeFrom buildVolumeFrom(Config volumeFromConfig) {
            Config config = request.getConfig().getFactory().create(volumeFromConfig);

            VolumeFrom volumeFrom = new VolumeFrom()
                .withSourceContainer(config.get("source_container", String.class))
                .withReadOnly(config.get("read_only", Boolean.class));

            return volumeFrom;
        }

        private MountPoint buildMountPoint(Config mountPointConfig) {
            Config config = request.getConfig().getFactory().create(mountPointConfig);

            MountPoint mountPoint = new MountPoint()
                .withContainerPath(config.get("container_path", String.class))
                .withSourceVolume(config.get("source_volume", String.class))
                .withReadOnly(config.get("read_only", Boolean.class));

            return mountPoint;
        }

        private PortMapping buildPortMapping(Config portMappingConfig) {
            Config config = request.getConfig().getFactory().create(portMappingConfig);

            PortMapping portMapping = new PortMapping()
                .withContainerPort(config.get("container_port", Integer.class))
                .withHostPort(config.get("host_port", Integer.class))
                .withProtocol(config.get("protocol", String.class));

            return portMapping;
        }

        private LogConfiguration buildLogConfiguration(Config config) {
            LogConfiguration logConfiguration = new LogConfiguration()
                .withLogDriver(config.get("log_driver", String.class, null))
                .withOptions(config.getMapOrEmpty("options", String.class, String.class));

            return logConfiguration;
        }
    }
}
