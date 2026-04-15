# libs

go project libs

```shell
go get -u -d -v github.com/glibtools/libs
```

## mdb cache invalidation

When `gm2c` cache is enabled, you can clear a single-record cache entry without issuing an update:

```text
mdb.DB.ClearBeanCache(&User{ID: 123})
mdb.DB.ClearBeanCacheByID(&User{}, 123)
```