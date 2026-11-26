# Chaos campaign run 9

Seed 801348 · 40 rps for 4 min · a fault every 20 s · 2 workers · laptop (Docker, 4 vCPU).

**Invariant violations: 0**

| Accepted | Client gave up | Client retries | Final statuses | Webhooks | Drain | Replay | Drain after |
|---|---|---|---|---|---|---|---|
| 8986 | 0 | 0 | {'done': 8986} | {'delivered': 4405, 'none': 4581} | 0 s | replayed 0 request(s) | 0 s |

## Faults injected

```
   10s  downstream slow 3 s
   45s  restart rabbitmq
   72s  kill worker
   95s  tenant webhook endpoint down
  135s  kill dispatcher
  158s  kill dispatcher
  181s  freeze dispatcher
  213s  kill dispatcher
```

## Violations

None.
