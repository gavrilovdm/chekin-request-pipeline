# Chaos campaign run 8

Seed 700925 · 40 rps for 4 min · a fault every 20 s · 2 workers · laptop (Docker, 4 vCPU).

**Invariant violations: 0**

| Accepted | Client gave up | Client retries | Final statuses | Webhooks | Drain | Replay | Drain after |
|---|---|---|---|---|---|---|---|
| 8983 | 0 | 515 | {'done': 8983} | {'delivered': 4461, 'none': 4522} | 0 s | replayed 4 request(s) | 0 s |

## Faults injected

```
   10s  restart rabbitmq
   37s  restart postgres
   63s  downstream slow 3 s
   98s  restart postgres
  124s  freeze worker past the lease
  169s  downstream slow 3 s
  204s  tenant webhook endpoint down
```

## Violations

None.
