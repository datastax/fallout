# Remote Yamls

Fallout supports importing test yamls from a remote url. Use the `yaml_url` key
to specify the URL of the raw remote test yaml. 

```yaml
yaml_url: https://some-yaml-url
```

If specifying a remote yaml url, no other keys are permitted. The following
test yaml will cause an exception.

```yaml
yaml_url: https://some-yaml-url
ensemble:
    server:
        ...
```

Templated remote yamls are supported, but defaults for the template params must be 
located with the remote definition, not in the local yaml. 

For example, the following test will run correctly.

Local:
```yaml
yaml_url: https://point-to-the-following-remote-yaml
```
Remote:
```yaml
node_count: 1
---
ensemble:
  server:
    node.count: {{node_count}}
    ...
```

But the following test will fail.

Local:
```yaml
node_count: 1
---
yaml_url: https://point-to-the-following-remote-yaml
```
Remote:
```yaml
ensemble:
  server:
    node.count: {{node_count}}
    ...
```
