# Processes that Producer.py handles explained visually
```
GitHub API          Producer               Kafka Topic
    │                   │                       │
    │    (sover)        │                       │
    │                   │                       │
    │◄── GET /events ───│  (vaknar var 5:e min) │
    │─── 300 events ───►│                       │
    │                   │── produce(event) ────►│
    │                   │── produce(event) ────►│
    │                   │── produce(event) ────►│  (alla 300)
    │                   │                       │
    │                   │  (sover igen)         │
    │                   │                       │
```