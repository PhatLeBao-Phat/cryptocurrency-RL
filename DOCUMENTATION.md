# PROJ DOCUMENTATION
### 1. Knowledge tips and tricks

#### `[Airflow]` Setup Airflow in Docker 
[Airflow-Docker setup][https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html]


#### `[Python]` To turn a folder into a package to import everywhere

Tips to import from Parent path: [overflow] [[https://stackoverflow.com/questions/39176561/copying-files-to-a-container-with-docker-compose](https://stackoverflow.com/questions/714063/importing-modules-from-parent-folder)]


```toml
[project]
name = "ptdraft"
version = "0.1.0"
description = "My small project"

[build-system]
build-backend = "flit_core.buildapi"
requires = ["flit_core >=3.2,<4"]
```
You need to make a `.toml` file and put it into the parent folder where then inside you specify the packages. After that, you can specify `conda pip install -e .` in the folder. To explain, we import the package in editable mode so we can work and import at the same time. 

Also do not forget to add `_init_.py` to make it as the folder as a package.

#### `[Airflow-Docker]` To expand the airflow image 
```Dockerfile
FROM apache/airflow:2.10.4

COPY pyproject.toml .
```
Then after that we can just replace the `common-service` with `build: .`

#### `[Shell]` To copy file from on folder to another in Bash

```bash
cp --help
cp [source] [destination]
```

#### `[Docker]` To access and exec command in Docker as root user 
```bash
docker exec -u root -it <container-id> /bin/bash
```

#### `[Docker]` To run command syntax in `docker-compose.yml`
```bash
command: >
    bash -c "python manage.py migrate
    && python manage.py runserver 0.0.0.0:8000"

```

#### `[Docker]` To show output in Docker build time for `echo` command
The output you are showing is from buildkit, which is a replacement for the classic build engine that docker ships with. You can adjust output from this with the `--progress` option:

```
 --progress string         
Set type of progress output (auto, plain, tty). Use plain to show container output (default "auto")
```

Adding `--progress=plain` will show the output of the run commands that were not loaded from the cache. This can also be done by setting the BUILDKIT_PROGRESS variable:

```bash
export BUILDKIT_PROGRESS=plain
```

If you are debugging a build, and the steps have already been cached, add `--no-cache` to your build to rerun the steps and redisplay the output:
```
docker build --progress=plain --no-cache ...
```
If you don't want to use buildkit, you can revert to the older build engine by exporting `DOCKER_BUILDKIT=0` in your shell, e.g.:

```
DOCKER_BUILDKIT=0 docker build ...
```

or
```
export DOCKER_BUILDKIT=0
docker build ...
```

#### `[psql]` to start up psql shell and connect 
```bash
psql -h 2cryptocurrency-rl-postgres-1 -p 5432 -U airflow
```

note that the `-h` param is using the name of the docker container in the network here since in a docker network, you can access other containers in a namespace-like manner.

#### `[python]` to filter existing rows between 2 df 
```python
# Merge and filter for new records
        merged_df = df.merge(db_df, on=unique_key, how="left", indicator=True)
        return merged_df[merged_df["_merge"] == "left_only"].drop(columns=["_merge"])
```

Such a good trick that we should all memorize