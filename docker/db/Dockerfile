FROM golang:1.10.2-stretch

WORKDIR /go/src/github.com/codingbeard/gatabase

COPY . .

RUN go get github.com/derekparker/delve/cmd/dlv

EXPOSE 8080 40000

CMD ["sleep", "infinity"]

#CMD ["dlv", "--listen=:40000", "--headless=true", "--api-version=2", "exec", "/go/src/github.com/codingbeard/gatabase/gatabase"]
