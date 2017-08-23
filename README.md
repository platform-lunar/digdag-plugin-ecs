# digdag-plugin-ecs

## Description

digdag-plugin-ecs is a plugin for submitting tasks to [Amazon Elastic Container Service](https://aws.amazon.com/ecs/).

## Requirements

- [Digdag](https://www.digdag.io/)

## Usage

```yaml
_export:
  plugin:
    repositories:
      - https://jitpack.io
    dependencies:
      - com.platformlunar.digdag.plugin:digdag-plugin-ecs:0.1.0

+ecs_register_action:
  ecs_register>: Register dummy application
  family: digdag-ecs-plugin-test
  container_definitions:
    - name: digdag-ecs-plugin-app-test
      image: alpine:3.6
      cpu: 256
      memory: 256
      log_configuration:
        log_driver: syslog

+import:
  _parallel: true

  +ecs_run_action1:
    ecs_run>: Run dummy ECS application1
    cluster: ${cluster}
    count: 1
    overrides:
      container_overrides:
        - name: digdag-ecs-plugin-app-test
          command:
            - /bin/echo
            - "hello"
    task_definition: ${ecs.last_task_family}

  +ecs_run_action2:
    ecs_run>: Run dummy ECS application2
    cluster: ${cluster}
    count: 1
    overrides:
      container_overrides:
        - name: digdag-ecs-plugin-app-test
          command:
            - /bin/echo
            - "world"
    task_definition: ${ecs.last_task_family}
```

Submission example:

```
digdag run --project sample plugin.dig -p cluster=<ecs_cluster_name> -p repos=`pwd`/build/repo --rerun
```

Be sure to set secrets like: `aws.ecs.access_key_id`, `aws.ecs.secret_access_key` and `aws.ecs.region` (or `aws.region`).

## License

[Apache License 2.0](LICENSE)
