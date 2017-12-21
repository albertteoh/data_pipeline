# Running in Docker

## Pre-requisites - Install docker
Install Docker and Docker Compose on your machine (Mac or Windows)

- https://docs.docker.com/engine/installation/

## Helper RPM's

Download these Oracle rpm's and place in ```docker/rpm_tmp``` folder

- oracle-instantclient12.2-basic-12.2.0.1.0-1.x86_64.rpm 
- oracle-instantclient12.2-devel-12.2.0.1.0-1.x86_64.rpm



## Start the Pipeline Stack


``` 
cd ./demonstration
./setup_everything
```

## Run An InitSync

```
docker-compose exec data-pipeline /bin/bash
cd /usr/local/data_pipeline/
python -m data_pipeline.initsync_pipe --config demonstration/demo_initsync_config.yaml
```
 
 
## Run Extractor

```
docker-compose exec data-pipeline /bin/bash
cd /usr/local/data_pipeline/
python -m data_pipeline.extract --config demonstration/demo_extractor_config.yaml
``` 
 
## Run Applier

```
docker-compose exec data-pipeline /bin/bash
cd /usr/local/data_pipeline/
python -m data_pipeline.apply --config demonstration/demo_applier_config.yaml
``` 
 
## Run the Web UI

- visible on localhost:5000

```
docker-compose exec data-pipeline /bin/bash
cd /usr/local/data_pipeline/
python -m ui.app.app --audituser postgres:password@db-postgres-audit:5432/myaudit  --httpport 5000
```

 


