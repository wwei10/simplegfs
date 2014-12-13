# SGFS

## How to get the source code?

- Set ```GOPATH``` by ```export GOPATH=/path/to/your/workspace```
- Type ```go get github.com/wweiw/simplegfs```
- ```$GOPATH/src/github.com/wweiw/simplegfs``` contains stable code that is
  code reviewed

Note: You can ```git checkout dev``` to view features that are under
developement and are no code reviewed yet

## How to run a demo?

Branch ```demo``` contains a demo to demonstrate how to use SGFS.

- ```git checkout demo```
- ```cd demo```

To start a master,
```
cd master
go run master.go
```

To start several chunk servers,
```
cd chunkserver
go run chunkserver.go :5555 &
go run chunkserver.go :6666 &
go run chunkserver.go :7777 &
```

To start an interactive client application,
```
cd client
go run client.go
```

## How to use the interactive client?

- ```read path off len```: read a file ```path``` at offset ```off``` with
  length ```len```
- ```write path off string```: write a file ```path``` at offset ```off```
  with content ```string```
- ```append path string```: append to file ```path``` with content
  ```string```, it will return the offset the content is written
- ```create path```: create a new file named ```path```
- ```mkdir path```: make a new directory named ```path```
- ```list path```: list all directories under ```path```
