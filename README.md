### Parquet Data Generator and Reader:

This will generate synthetic invoice data (see Challenge.txt) and print some of it to stdout.  
The data is read and written in parallel depending on the number of CPUs available.

Usage:

1. cd into parquetDataGenerator
2. go get ./...
3. go run .
4. check parquet.file in the root of the folder

### Arrow Flight Server and Client:

It also contains a flight server and client.
Only methods that will be used by the client are implemented.
It PUTs a record of arrow data and then GETs the same from the server.

Usage:

1. cd into arrowFlightServerGo
2. go get ./...
3. go run .
4. In another shell, cd into arrowFlightClient
5. go get ./...
6. go run .
7. Observe the stdout for the echoed data
