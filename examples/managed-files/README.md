# Managed Files

Fallout can fetch files from multiple external sources and use them within a test. There are two types of files Fallout will manage: remote files and local files.

Remote files are present on remote nodes, thus are inherently tied to the node group lifecycle. To do this, there is the `remote_files` Configuration Manager.

Local files are present on the Fallout machine itself, and are available throughout the entire test run. They are defined on the node group as `local_files`.

For both, the files themselves are specified through a FileSpec which can pull data from multiple kinds of external sources.

Both will save a copy of the managed files under a `managed_files` directory in the nodegroup's artifacts.

## Remote Files Configuration Manager

Files on remote machines can be reused between test runs. They reside in the `fallout_library` directory on remote nodes.

An simple example:
```yaml
      configuration_manager:
      - name: remote_files
        properties:
          files:
            - path: file.txt
              data: |
                contains content
```

## Local Files

`local_files` is a top-level node group key which accepts a list of file specs. For example:

```yaml
  server:
      local_files:
          - path: file.txt
            data: |
              contains content
```

At the time of writing, the only valid usage of local files is with kubernetes manifests.

## Specifying Files

### Path

`path` specifies the name of the file and a path to the file _in a sub-directory_ of the root file location.
For example, `path: /some/sub/dir/file.txt` will save `file.txt` under `<root files location>/some/sub/dir/`. Giving no value for `path` will save the file directly under the root file location.

The root file location depends on if the files are local or remote, and is provisioner specific in the remote case.

| Type | Root File Location | Note |
|------|--------------------|------|
| Local | \<test run's artifacts\>/\<node group of file\>/managed_files/ | |
| Remote | $FALLOUT_LIBRARY_DIR/ | The fallout library dir is provisioner dependent. Commonly it is under the user's home, i.e. `~/fallout_library` |

Note: for all FileSpec type _other than Git_ a file name _must_ be specified.

`path` provides uniqueness for files with the same name. For example:
```yaml
      configuration_manager:
      - name: remote_files
        properties:
          files:
            - path: a/file.txt
              data: |
                contains content A
            - path: b/file.txt
              data: |
                contains content B
```
Each can be uniquely identified by `<<file:a/file.txt>>` or `<<file:b/file.txt>>`.

### FileSpec

A FileSpec is a map with two entries. Every FileSpec has a `path` property in addition to a unique type property to describe the content of the file.

There are five type of FileSpecs, corresponding to the different sources Fallout will fetch the file from. Each FileSpec has slightly different properties.
Examples of the different properties can be found in the [files configuration manager example.](files-configuration-manager.yaml)

Examples of all file specs can be found in [files-configuration-manager.yaml](files-configuration-manager.yaml).

- data: writes raw data to a file
    - `data` property: string or literal style to be output to the file. See [yaml spec](https://yaml.org/spec/1.2/spec.html#id2795688) for exact details.
- yaml: writes to a yaml file
    - `yaml` property: valid yaml to output to file.
- json: writes to a json file
    - `json` property: valid yaml to output to file. Will be transformed into a json object before being written to the file.
- url: Reads content directly from a url
    - `url` property: the url where the file content exists.
- git: shallow clones an entire repository
    - `git` property: a map consisting of required key `repo` and optional key `branch`.

## Using Managed Files

Managed files are identified in the test yaml by wrapping the path in `<<file:file-to-use.txt>>`. In the case of a git repo, `file-to-use.txt` must be the full path within the repo.
