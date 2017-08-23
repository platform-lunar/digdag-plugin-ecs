package com.github.platformlunar.digdag.plugin.ecs;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;

import io.digdag.client.config.Config;
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
import com.amazonaws.services.ecs.model.KeyValuePair;
import com.amazonaws.services.ecs.model.DescribeTasksRequest;
import com.amazonaws.services.ecs.model.DescribeTasksResult;
import com.amazonaws.services.ecs.model.PlacementConstraint;
import com.amazonaws.services.ecs.model.PlacementStrategy;
import com.amazonaws.services.ecs.model.Task;
import com.amazonaws.services.ecs.model.RunTaskRequest;
import com.amazonaws.services.ecs.model.ContainerOverride;
import com.amazonaws.services.ecs.model.TaskOverride;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.List;

import static io.digdag.standards.operator.state.PollingRetryExecutor.pollingRetryExecutor;
import static io.digdag.standards.operator.state.PollingWaiter.pollingWaiter;

import static java.util.stream.Collectors.toList;

public class EcsRunOperatorFactory implements OperatorFactory
{

    private final TemplateEngine templateEngine;
    private final Map<String, String> environment;

    private static final String STATE_SUBMIT = "submit";
    private static final String STATE_WAIT = "wait";
    private static final String STATE_POLL = "poll";

    private static Logger logger = LoggerFactory.getLogger(EcsOperator.class);

    public EcsRunOperatorFactory(TemplateEngine templateEngine, @Environment Map<String, String> environment)
    {
        this.templateEngine = templateEngine;
        this.environment = environment;
    }

    public String getType()
    {
        return "ecs_run";
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

            RunTaskRequest runTaskRequest = buildRunTaskRequest(config);

            @SuppressWarnings("unchecked")
            List<String> taskArns = pollingRetryExecutor(state, STATE_SUBMIT)
                .withErrorMessage("ECS task %s failed to register", runTaskRequest.getTaskDefinition())
                .runOnce(List.class, (TaskState state) -> {
                        logger.info("Running ECS task {}", runTaskRequest.getTaskDefinition());

                        try {
                            List<String> submittedTasks = client.runTask(runTaskRequest)
                                .getTasks()
                                .stream()
                                .map(t -> t.getTaskArn())
                                .collect(toList());

                            return submittedTasks;
                        } catch (com.amazonaws.AmazonServiceException ex) {
                            throw new TaskExecutionException(ex);
                        }
                    });

            waitForTaskCompletion(client, config.get("cluster", String.class), taskArns);

            return TaskResult.defaultBuilder(request).build();
        }

        private void waitForTaskCompletion(AmazonECSClient client, String cluster, List<String> taskArns) {
            String lastTaskArn = Iterables.getLast(taskArns);

            pollingWaiter(state, STATE_WAIT)
                .withWaitMessage("ECS tasks still running")
                .withPollInterval(DurationInterval.of(Duration.ofSeconds(1), Duration.ofSeconds(10)))
                .awaitOnce(String.class, pollState -> checkTaskCompletion(client, cluster, lastTaskArn, pollState));
        }

        private Optional<String> checkTaskCompletion(AmazonECSClient client, String cluster, String taskArn, TaskState pollState) {
            return pollingRetryExecutor(pollState, STATE_POLL)
                .withRetryInterval(DurationInterval.of(Duration.ofSeconds(5), Duration.ofSeconds(15)))
                .run(s -> {
                        DescribeTasksRequest describeTask = new DescribeTasksRequest()
                            .withTasks(taskArn)
                            .withCluster(cluster);
                        DescribeTasksResult describeResult = client.describeTasks(describeTask);
                        Task taskState = Iterables.getLast(describeResult.getTasks());
                        String currentTaskState = taskState.getLastStatus();

                        switch (currentTaskState) {
                            case "PENDING":
                            case "RUNNING":
                                return Optional.absent();
                            case "STOPPED":
                                String stoppedReason = taskState.getStoppedReason();

                                List<String> containerStopReasons = taskState
                                    .getContainers()
                                    .stream()
                                    .filter((container) -> {
                                            return java.util.Optional.of(container)
                                                .map(c -> c.getReason())
                                                .map(String::toLowerCase)
                                                .map(reason -> reason.contains("error"))
                                                .orElse(false);
                                        })
                                    .map((container) -> container.getReason())
                                    .collect(toList());

                                if (stoppedReason.toLowerCase().contains("error")) {
                                    throw new TaskExecutionException("ECS task failed with reason: " + stoppedReason);
                                } else if (!containerStopReasons.isEmpty()) {
                                    throw new TaskExecutionException("ECS containers failed with reasons: " + String.join(", ", containerStopReasons));
                                } else {
                                    return Optional.of(currentTaskState);
                                }
                            default:
                                throw new RuntimeException("Unknown ECS task state: " + currentTaskState);
                        }
                    });
        }

        private RunTaskRequest buildRunTaskRequest(Config config) {
            List<PlacementConstraint> placementConstraints = config.getListOrEmpty("placement_constraints", Config.class)
                .stream()
                .map(this::buildPlacementConstraint)
                .collect(toList());

            List<PlacementStrategy> placementStrategies = config.getListOrEmpty("placement_strategies", Config.class)
                .stream()
                .map(this::buildPlacementStrategy)
                .collect(toList());

            Config overrides = config.getNestedOrGetEmpty("overrides");

            RunTaskRequest runTaskRequest = new RunTaskRequest()
                .withCluster(config.get("cluster", String.class))
                .withTaskDefinition(config.get("task_definition", String.class))
                .withCount(config.get("count", Integer.class, 1))
                .withStartedBy(config.get("started_by", String.class, "digdag"))
                .withGroup(config.get("group", String.class, null))
                .withPlacementConstraints(placementConstraints)
                .withPlacementStrategy(placementStrategies);

            if (!overrides.isEmpty()) {
                TaskOverride taskOverride = buildTaskOverride(overrides);

                runTaskRequest.withOverrides(taskOverride);
            }

            return runTaskRequest;
        }

        private PlacementConstraint buildPlacementConstraint(Config placementConstraintConfig)
        {
            Config config = request.getConfig().getFactory().create(placementConstraintConfig);

            PlacementConstraint placementConstraint = new PlacementConstraint()
                .withExpression(config.get("expression", String.class))
                .withType(config.get("type", String.class));

            return placementConstraint;
        }

        private PlacementStrategy buildPlacementStrategy(Config placementStrategyConfig)
        {
            Config config = request.getConfig().getFactory().create(placementStrategyConfig);

            PlacementStrategy placementStrategy = new PlacementStrategy()
                .withType(config.get("type", String.class))
                .withField(config.get("field", String.class));

            return placementStrategy;
        }

        private TaskOverride buildTaskOverride(Config taskOverrideConfig)
        {
            Config config = request.getConfig().getFactory().create(taskOverrideConfig);

            List<ContainerOverride> overrides = config.getListOrEmpty("container_overrides", Config.class)
                .stream()
                .map(this::buildContainerOverride)
                .collect(toList());

            TaskOverride taskOverride = new TaskOverride()
                .withTaskRoleArn(config.get("task_role_arn", String.class, null))
                .withContainerOverrides(overrides);

            return taskOverride;
        }

        private KeyValuePair buildKvPair(Config kvPairConfig) {
            Config config = request.getConfig().getFactory().create(kvPairConfig);

            KeyValuePair kvPair = new KeyValuePair()
                .withName(config.get("name", String.class))
                .withValue(config.get("value", String.class));

            return kvPair;
        }

        private ContainerOverride buildContainerOverride(Config containerOverrideConfig)
        {
            Config config = request.getConfig().getFactory().create(containerOverrideConfig);

            List<KeyValuePair> kvPairs = config.getListOrEmpty("environment", Config.class)
                .stream()
                .map(this::buildKvPair)
                .collect(toList());

            ContainerOverride containerOverride = new ContainerOverride()
                .withName(config.get("name", String.class, null))
                .withMemory(config.get("memory", Integer.class, null))
                .withMemoryReservation(config.get("memory_reservation", Integer.class, null))
                .withCpu(config.get("cpu", Integer.class, null))
                .withCommand(config.getListOrEmpty("command", String.class))
                .withEnvironment(kvPairs);

            return containerOverride;
        }
    }
}
