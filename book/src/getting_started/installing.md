# Installing

Setting up lnx is pretty simple overall, you will want either a copy of the docker image
or build from source to start using it.

## Building from source
You will need a newish version of rust for this, this project was built on rustc `rustc 1.52.1`
to be specific so any version beyond this should be alright.

- Download the file via `git clone https://github.com/ChillFish8/lnx.git` and enter the downloaded 
folder.
- Run `cargo build --release`
- Extract the exported binary from the `target/release` folder.

*alternatively you can use `cargo run --release -- <flags>` if you want to avoid the 
job of extracting the built binary*

## Running from docker
Docker images are pre-built following the `master` branch, this becomes the `latest`
docker image tag.

#### Running via docker CLI
```bash
docker run chillfish8/lnx:latest -p "8000:8000" -e "AUTHORIZATION_KEY=hello" -e "LOG_LEVEL=info" 
```

*Note: Running without a persistent volume will mean no data will be kept
between a restart, if you intend on deploying this it is **HIGHLY** advised
you mount a volume.*

#### Running via docker-compose
```yaml
version: '3'

services:
    lnx:
      image: chillfish8/lnx:latest
      ports:
        - "8000:8000"
      volumes:
        - "/my/dir:/lib/lnx" 
      environment:
        - AUTHORIZATION_KEY=hello
        - LOG_LEVEL=info
```
