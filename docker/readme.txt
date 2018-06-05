To run the docker container:

    docker-compose -f docker-compose.yml -f docker-compose-dev.yml up

To ssh into the container (in a new tab):

    docker exec -it gatabase_db_1 /bin/bash

To run unit tests outside of the container:

    docker exec -it gatabase_db_1 go test ./...

To run unit tests inside the container:

    go test ./...

To debug unit tests inside the container:

    cd folder/to/test
    dlv --headless --api-version=2 --listen=:40000 test
    *start debugger on port 40000*

To debug the application:

    cd /go/src/github.com/codingbeard/gatabase
    go build -gcflags='-N -l' &&  dlv --listen=:40000 --headless=true --api-version=2 exec ./gatabase
    *start debugger on port 40000*

To stop debugging (in a new tab):

    pkill -f "dlv"