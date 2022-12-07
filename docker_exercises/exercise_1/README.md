# Data Minded Academy - Containerization with Docker
## Exercise 1 - Basics of the Docker CLI

In this exercise, you will have to use the `docker` CLI to complete a few simple operation tasks. The tasks are the following:

1. Run a container based on the `hello-world:latest` image. Name the container `my-hello-world-container`.
2. Run three containers with the same terminal (you will need the detached mode). Each one will be based on the following ever-running images:
    * `nginx:1.21.5`
    * `postgres:13.2-alpine` with the following environment variables:
        * `POSTGRES_USER`: abcd
        * `POSTGRES_DB`: abcd
        * `POSTGRES_PASSWORD`: helloworld1234
    * `grafana/grafana:latest` (create a port mapping from `3000` inside the container to the port of your choice outside)
    
3. Once running, try to access the Grafana UI using the Remote Explorer menu in the Gitpod UI.
4. List the available images and the existing containers (running or stopped).
5. To free up some space, clean the Docker daemon from all containers and images (delete everything).