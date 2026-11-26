# Chaos campaign run 10

Seed 602821 · 40 rps for 4 min · a fault every 20 s · 2 workers · laptop (Docker, 4 vCPU).

**Invariant violations: 0**

| Accepted | Client gave up | Client retries | Final statuses | Webhooks | Drain | Replay | Drain after |
|---|---|---|---|---|---|---|---|
| 8799 | 0 | 563 | {'done': 8799} | {'delivered': 4453, 'none': 4346} | 0 s | replayed 0 request(s) | 0 s |

## Faults injected

```
   10s  restart rabbitmq
   37s  kill api
   59s  restart postgres
   85s  freeze worker past the lease
  131s  restart rabbitmq
  158s  downstream slow 3 s
  193s  kill api
  216s  restart postgres
```

## Violations

None.
