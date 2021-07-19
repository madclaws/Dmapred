# Dmapred

**Distributed MapReduce Systems in Elixir**

## Running
```
mix deps.get && mix compile
```
NOTE: **Master and all workers should be run on different terminals/windows**.

### Starting Master

``` make start_master```

### Starting Workers
``` make start_worker target WORKER=worker1 ```

``` make start_worker target WORKER=worker2```

``` make start_worker target WORKER=worker3```

``` make start_worker target WORKER=worker4```



