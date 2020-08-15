# flink_join_test

A test project for join operation by Apache Flink.

## Required

- [sbt](https://www.scala-sbt.org/index.html)

## Run

For the join test by DataStream API.
(blog post: [Apache Flink の Broadcast State Pattern を用いた stream data と static data の join](https://soonraah.github.io/flink-join-by-broadcast-state-pattern/))

```
sbt 'runMain example.FlinkJoinTest'
```

For the join test by Table API.
(blog post: [Apache Flink の Temporary Table Function を用いた stream data と static data の join](https://soonraah.github.io/flink-join-by-temporal-table-function/))

```
sbt 'runMain example.FlinkTableJoinTest'
```
