# Multiple nodegroup examples

This directory includes examples of tests that use multiple server groups or multiple client groups. 


Typically, specifying an ensemble looks like this:
```
ensemble:
  server:
    [properties]
  client:
    [properties]
```

Specifying multiple servers is easy. Rather than using the `server` key in your `ensemble` maps, use `servers`. Then, specify a yaml list. Each item in the list will correspond to a single server group.
Each server group must take a name, in addition to the usual properties, so that we can inform modules which server group[s] they are operating against. Server groups can be on different providers, can have different topologies, and can have different instance types. DSE clusters cannot span multiple server groups; each server group is exactly one DSE cluster. A common use case for multiple server groups is to test advanced replication.
```
ensemble:
  servers:
    - name: source
      [properties]

    - name: destination
      [properties]

  client:
    [properties]
```

Specifying multiple clients works in much the same way. Simply use the `clients` key, and give each client group a name. A common use case for multiple clients is to load test with heterogeneous hardware. Perhaps you want to generate a large workload, and need many clients, but you only need to measure client statistics from a single machine. You could specify one client group with a single ironic node, and another client group with many nebula nodes.

```
ensemble:
  server:
    [properties]
  clients:
    - name: load_generator
      [properties]

    - name: measurement
      [properties]
```


It is completely possible to utilize multiple server groups and multiple client groups in the same yaml. Do not use this feature to run multiple logically independent tests within one fallout yaml.
```
ensemble:
  servers:
    - name: source
      [properties]

    - name: destination
      [properties]
    
  clients:
    - name: load_generator
      [properties]

    - name: measurement
      [properties]
```
