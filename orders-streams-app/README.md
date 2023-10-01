### (Optional) Containerize the Native Executable on Linux

The following steps (1-4) are for Linux only.

1. On Linux, you can easily containerise the native executable using the following command:

    ```shell
    docker build -f Dockerfiles/Dockerfile.native --build-arg APP_FILE=orders-streams-app -t orders-streams-app:native.0.0.1-SNAPSHOT .
    ```

2. Run the application:

    ```shell
    docker run --rm --name native -p 8080:8080 orders-streams-app:native.0.0.1-SNAPSHOT
    ```

3. Open the application [http://localhost:8080/jibber](http://localhost:8080/jibber) in a browser, or from a new terminal window, call the endpoint using `curl`:

    ```shell
    curl http://localhost:8080/jibber
    ```

   It should generate a random nonsense verse in the style of the poem Jabberwocky by Lewis Carrol.

4. To stop the application, first get the container id using `docker ps`, and then run:

    ```shell
    docker rm -f <container_id>
    ```

### (Optional) Use Multistage Docker Builds to Build a Native Image and Package it in a Lightweight Container

The following steps (5-8) are for all platforms - MacOS, Windows, and Linux.

For MacOS and Windows, to build a Docker image containing your native executable, you need to build the native executable inside a Docker container. To do this, we've provided a [multistage Docker build file](./Dockerfiles/Dockerfile).

5. Run this command to build the native executable within a Docker container:

    ```shell
    docker build -f Dockerfiles/Dockerfile -t orders-streams-app:native.0.0.1-SNAPSHOT .
    ```

6. Run the application:

    ```shell
    docker run --rm --name native -p 8080:8080 orders-streams-app:native.0.0.1-SNAPSHOT
    ```

7. Open the application [http://localhost:8080/actuator/health](http://localhost:8080/actuator/health) in a browser, or from a new terminal window, call the endpoint using `curl`:

    ```shell
    curl http://localhost:8080/actuator/health
    ```

    It should generate a random nonsense verse in the style of the poem Jabberwocky by Lewis Carrol.

8. To stop the application, first get the container id using `docker ps`, and then run:

    ```shell
    docker rm -f <container_id>
    ```