# CS223 Project: Two-phase commit protocol implementation

## Installation

### Set up databases

PostgreSQL must be installed on the system for the databases.
We need `N` independent PostgreSQL server instances to act as
seperate nodes. Hence, create seperate databases (with their
own data directories) using

    initdb -D /path/to/data/directory

Then start the servers using

    pg_ctl start -D /path/to/data/directory

The port to run the server on can be given in `postgresql.conf` in
the data directory or using the `PGPORT` environment variable:

    env PGPORT=1234 pg_ctl start -D /path/to/data/directory

### Set up Python environment

Run with Python 3. Required package:

* `psycopg2`: Python Interface to PostgreSQL
   
  To install in a virtual environment on macOS, the library path
  for lib SSL might be explicitly provided as such:
  
      env LDFLAGS="-I/usr/local/opt/openssl/include -L/usr/local/opt/openssl/lib" pip --no-cache install psycopg2