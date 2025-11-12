# Implementation-of-Ricart-Argawala

Each GO process can be started with the command go run . {nodeId} {nodeAdress} {peersNodeAdress (comma sepeerated)}

As an example, to run the program open three terminal windows and enter the following (in the path grpc/node):

go run . 3 localhost:5003 localhost:5001,localhost:5002

go run . 1 localhost:5001 localhost:5002,localhost:5003  

go run . 2 localhost:5002 localhost:5001,localhost:5003

