# redis_fdw

## redis docker init

```
docker run -d --name redis-server -p 8899:6379 redis
```

## Redis Supported Type

* Hash

## execute command.

```
CREATE EXTENSION redis_fdw_rs;

create foreign data wrapper redis_wrapper handler redis_fdw_handler;
  
create server redis_server foreign data wrapper redis_wrapper
options (
  host_port '127.0.0.1:8899'
);

CREATE USER MAPPING FOR PUBLIC SERVER redis_server OPTIONS (password 'secret');

CREATE FOREIGN TABLE redis_db0 (key text, value text) 
	SERVER redis_server
	OPTIONS (database '0' , table_type 'hash', table_key_prefix 'mytable:');
```