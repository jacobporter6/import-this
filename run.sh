#!/bin/bash
docker run \
    -v "$(pwd)/volumes/data:/var/opt/mssql/data" \
    -e "ACCEPT_EULA=Y" \
    -e "SA_PASSWORD=jP123456!" \
    -p 1433:1433 \
    --name Bart \
    -d mcr.microsoft.com/mssql/server:2019-latest
