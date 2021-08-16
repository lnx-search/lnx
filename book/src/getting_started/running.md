# Running
Running a default version of lnx can be done simply using the following commands:

#### Running
```bash
lnx <flags>
```
This will start the sever in a default configuration.

*NOTE: It is generally recommend to customise the running on lnx for production and optimum systems,
see [**optimising lnx**](optimising.md) for information on why it matters for performance sake.* 

#### Help
```bash
lnx --help
``` 
This will bring up a detailed help command for 
all CLI options (including environment options.)

## Optional Flags
lnx provides a wide set of command line arguments to customise the running
of the server.

### Authentication Key
If specified this will require an authentication key on each request. 
 
Generally, it's recommended to have this in a production environment.

As CLI:
```bash
-a, --authentication-key <authentication-key>
```

As environment key:
```
AUTHENTICATION_KEY=<key>
```


### Server Host
The host to bind to (normally: '127.0.0.1' or '0.0.0.0'.) 

The default is `127.0.0.1`

As CLI:
```bash
-h, --host <host>
```

As environment key:
```
HOST=<host>
```


### Server Port
The port to bind the server to.

The default is `8000`

As CLI:
```bash
-p, --port <port>
```

As environment key:
```
PORT=<port>
```


### Log File
A optional file to send persistent logs.

This should be given as a file path.

As CLI:
```bash
--log-file <log-file>
```

As environment key:
```
LOG_FILE=<log-file>
```


### Log Level
The log level filter, any logs that are above this level wont be displayed.

Defaults to `info`

As CLI:
```bash
--log-level <log-level>
```

As environment key:
```
LOG_LEVEL=<log-level>
```


### Pretty Logs
An optional bool to use ASNI colours for log levels. 
You probably want to disable this if using file based logging.

Defaults to `true`

As CLI:
```bash
--pretty-logs <pretty-logs>
```

As environment key:
```
PRETTY_LOGS=<pretty-logs>
```


### Pretty Logs
An optional bool to use ASNI colours for log levels. 
You probably want to disable this if using file based logging.

Defaults to `true`

As CLI:
```bash
--pretty-logs <pretty-logs>
```

As environment key:
```
PRETTY_LOGS=<pretty-logs>
```


### Runtime Threads
The number of threads to use for the [tokio](https://tokio.rs) runtime.


If this is not set, the number of logical cores on the machine is used.

As CLI:
```bash
-t, --runtime-threads <runtime-threads>
```

As environment key:
```
RUNTIME_THREADS=<runtime-threads>
```


### TLS Cert File
If specified this will be used in the TLS config for HTTPS. 

If this is specified the [TLS Key File](/getting_started/running.html#tls-key-file) must also be given.

As CLI:
```bash
--tls-cert-file <tls-cert-file>
```

As environment key:
```
TLS_CERT_FILE=<tls-cert-file>
```


### TLS Key File
If specified this will be used in the TLS config for HTTPS. 

If this is specified the [TLS Cert File](/getting_started/running.html#tls-cert-file) must also be given.

As CLI:
```bash
--tls-key-file <tls-key-file>
```

As environment key:
```
TLS_KEY_FILE=<tls-key-file>
```