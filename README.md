# crash-safe-retry-e2e

Canonical **sqlite** simulated-engine harness for Declarative Migrations, focused on **crash-safe retry, lock ownership, and checksum refusal**.

This closes a DEN-3447 gap that existing suites do not execute as a real harness:

- `idempotent-replay-e2e` currently ships a declarative recovery **contract**, not process-crash injection.
- `failure-injection-atomicity` covers late uniqueness failure, not the four crash windows.
- `concurrent-migrator-lock` and `postgres-lock-contention-e2e` cover live contention, not stolen/stale tokens plus checksum refusal.

The engine uses a disposable local SQLite file. There are no production credentials.

## Fault points

1. crash before the DDL transaction
2. crash during DDL (transaction rolls back)
3. crash after DDL commit but before the history write
4. crash after the history write but before client acknowledgement

Retry of each window must converge to one schema/history state without double-applying DDL. Stolen or stale locks and edited-history checksum mismatches refuse further writes. Recovery guidance is generated from durable machine state.

```bash
PYTHONPATH=src python3 -m unittest -v tests/test_contract.py tests/test_engine_focus.py tests/test_engine_e2e.py
```
