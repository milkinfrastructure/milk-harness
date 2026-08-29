# Bridge reference

Milk Harness temporarily contains the deterministic reconciliation used by Milk
Man. The active interfaces are:

- [`../../README.md`](../../README.md): data flow, configuration, and limits;
- [`production-scheduler.md`](production-scheduler.md): the manual verification
  and rollback entry point.

Milk Man owns scheduling. Milk Carton owns capture and route publication. This
repository does not authorize route signing, arbitrary provider work, or a
resident service.
