# Rummage : Redis Instance

This directory contains files relating to the instance of a Redis server that
is required for the project.

## Manually building and running the Docker image

To run the docker image you will firstly need to build it:

`docker build -t rummage_redis -f docker\Dockerfile.redis .`

Once this has been build you will need to run it:

`docker run --name rummage_redis -d -e REDIS_PASSWORD=<PASSWORD> -p 6379:6379 -v <REDIS_DATA_PATH>:/data rummage_redis`

> [!NOTE]
> PASSWORD : The password that will be required for authentication purposes
> REDIS_DATA_PATH : The full path to the directory where redis data is stored (e.g. /rummage/redis_data)

### Useful Docker commands

Stop Docker : docker stop rummage_redis
- Remove Docker container : docker rm rummage_redis
- Remove Docker image : docker rmi rummage_redis
- Get Docker Logs : docker logs rummage_redis
