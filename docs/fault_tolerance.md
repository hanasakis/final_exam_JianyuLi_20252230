# Fault Tolerance Test

## Test Objective

Demonstrate that the 3-broker Kafka cluster handles a broker failure gracefully through leader re-election.

## Cluster Configuration

- **Brokers:** kafka1 (node 1), kafka2 (node 2), kafka3 (node 3)
- **Topic:** sensor-events — 3 partitions, replication factor 3
- **min.insync.replicas:** 2

## Initial State (All brokers healthy)

```bash
$ docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 \
    --describe --topic sensor-events

Topic: sensor-events  PartitionCount: 3  ReplicationFactor: 3
  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
```

**Observations:**
- All three partitions have healthy leaders distributed across brokers.
- ISR (In-Sync Replicas) matches the full replica set for every partition.
- Leaders are evenly distributed: partition 0 → broker 1, partition 1 → broker 2, partition 2 → broker 3.

## Fault Injection

Stop broker 2 (kafka2) to simulate a node failure:

```bash
$ docker stop kafka2
```

## State After Failure

```bash
$ docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 \
    --describe --topic sensor-events

Topic: sensor-events  PartitionCount: 3  ReplicationFactor: 3
  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,3
  Partition: 1  Leader: 1  Replicas: 2,3,1  Isr: 3,1
  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1
```

**Post-failure observations:**
- Partition 0: Leader remains on broker 1, ISR reduced to {1, 3} (broker 2 removed).
- Partition 1: Leader re-elected to broker 1 (was on broker 2). ISR updated to {3, 1}.
- Partition 2: Leader remains on broker 3, ISR updated to {3, 1}.
- **min.insync.replicas=2 is still satisfied** — producers can continue writing with acks=all.
- No data loss: replication factor 3 means each partition still has at least 2 replicas.

## Recovery

Restart broker 2:

```bash
$ docker start kafka2
```

After a brief re-sync period, the ISR returns to {1, 2, 3} for all partitions. Leaders remain at their newly elected positions (no automatic rebalance back).

## Conclusion

The cluster tolerates a single broker failure without data loss and without interrupting producer operations. With `min.insync.replicas=2`, the system requires only 2 of 3 brokers to acknowledge writes, providing a balance between durability and availability.
