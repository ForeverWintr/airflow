## Goal

Build a minimal, self-contained reproduction of a bug in Airflow's notifiers, in the `apache/airflow` repo, as groundwork for an upstream issue and fix: **a DAG-level notifier instance is reused across several callbacks when the dag-processor batches callbacks for one DAG file, and because `BaseNotifier.__call__` renders template fields in place, every callback after the first sends the first run's rendered values.**

Deliverables (described at the end): a reproduction DAG with steps to run it in Breeze, a failing unit test, and a draft issue. **Do not open issues or PRs, push branches, or comment on GitHub.** Hand everything back to me for review first.

## Background (observed on Airflow 3.2.1; check `main` before assuming it still applies)

1. **Rendering happens in place.** `BaseNotifier.__call__` (`task-sdk/src/airflow/sdk/bases/notifier.py`) calls `self._update_context(context)`, then `self.render_template_fields(context)`, then `self.notify(context)`. `render_template_fields` replaces each attribute in `template_fields` with its rendered value (its docstring says it "mutates the attributes in-place and is irreversible"). After the first call, `self.subject` holds a plain string like `"Failed: run_A"`. Rendering it again does nothing. `__await__` (the async/triggerer path) has the same pattern. Some providers mutate further, e.g. `SmtpNotifier._build_email_content` fills `subject`/`html_content` from connection templates on the first call and renders them in place.

2. **DAG callbacks can share one instance.** In the dag-processor (`airflow-core/src/airflow/dag_processing/manager.py`):
   - `_add_callback_to_queue` appends each callback to `_callback_to_execute[DagFileInfo]`, a list per file.
   - `_create_process` pops the whole list for a file into one `DagFileProcessorProcess`.
   - `_start_new_processes` skips files that already have a running processor, so callbacks pile up while one is busy.
   - In `processor.py`, `_parse_file` builds one `BundleDagBag` and `_execute_callbacks` loops over every request. `_execute_dag_callbacks` calls `callback(context)` directly on the DAG's `on_success_callback`/`on_failure_callback` object, with no copy.
   - So several DAG runs of one file finishing while that file's processor is busy are handled by **one process, one DagBag, one notifier instance**.

3. **Reproduced on a KubernetesExecutor deployment of 3.2.1.** Config: one dag-processor replica, `max_callbacks_per_loop=1`, `parsing_processes=2`. Three manual runs triggered about 2s apart:
   ```
   pid=224 run=A rendered='run A' stale=False
   pid=256 run=B rendered='run B' stale=False  notifier_id=0x...8490
   pid=256 run=C rendered='run B' stale=True   notifier_id=0x...8490
   ```
   A later batch of three gave D correct, then E and F both rendered `'run D'`. Building a fresh notifier inside a plain callable for each call fixed it (all `stale=False`). The default `max_callbacks_per_loop` (20) should make batching *more* likely than in our setup.

   **Scope:** task-level callbacks normally run in the task's own process, so they're usually unaffected. `dag.test()` / in-process test runs also reuse the instance.

## Steps

1. **Check whether this is already known or fixed.** Search apache/airflow issues and PRs (e.g. "notifier render_template_fields", "on_success_callback notifier stale", "BaseNotifier copy", "callback batching dag processor"). Then read `main`'s versions of the files above: has `BaseNotifier.__call__`, or the processor's callback execution, changed since 3.2.1? Report what you find. If it's fixed on main, say where and stop there.

2. **Reproduction DAG.** Put it somewhere sensible for a local reproduction, such as `files/dags/` (Breeze mounts it), not anything that ships. It should use no external services:
   ```python
   import os, time
   import pendulum
   from airflow.providers.standard.operators.empty import EmptyOperator
   from airflow.sdk import DAG
   from airflow.sdk.bases.notifier import BaseNotifier

   class ReuseDemoNotifier(BaseNotifier):
       template_fields = ("message",)

       def __init__(self, message: str, sleep_seconds: int = 60):
           super().__init__()
           self.message = message
           self.sleep_seconds = sleep_seconds

       def notify(self, context):
           self.log.warning(
               "REUSE_DEMO run_id=%s rendered=%r stale=%s notifier_id=%s pid=%s",
               context["run_id"], self.message, context["run_id"] not in self.message,
               hex(id(self)), os.getpid(),
           )
           time.sleep(self.sleep_seconds)  # keep this file's processor busy so callbacks batch

   with DAG(
       dag_id="notifier_reuse_demo",
       start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
       schedule=None, catchup=False, max_active_runs=10, is_paused_upon_creation=False,
       on_success_callback=ReuseDemoNotifier(message="run {{ run_id }}"),
   ):
       EmptyOperator(task_id="noop")
   ```
   Run it in Breeze (`breeze start-airflow`, with a standalone dag-processor if that isn't the default; check the Breeze docs). Trigger 3+ runs a few seconds apart with unique run IDs:
   ```bash
   ts=$(date +%H%M%S); for i in 1 2 3; do airflow dags trigger notifier_reuse_demo -r "demo_${ts}_$i"; done
   ```
   Then grep `REUSE_DEMO` in the dag-processor's per-file log (under `logs/dag_processor/latest/...`). The bug is reproduced when a later run in the same pid logs `stale=True`.

   Judge by `stale`, not `notifier_id`: a freshly created instance can reuse a freed object's address. If Breeze's setup prevents batching, explain why and what config made it reproduce.

3. **Failing unit test**, in the repo's existing test conventions (likely next to the notifier tests in `task-sdk/tests/...`; find them). The minimal version:
   - Create a templated `BaseNotifier` subclass whose `notify` records `self.<field>`.
   - Call one instance with two contexts that have different `run_id`s.
   - Assert the second call saw the second run's value.

   It should fail on main if the bug is still there. If it's easy, also add a test at the processor level: two `DagCallbackRequest`s for one file through `_execute_callbacks` with the same DagBag.

4. **Sketch candidate fixes; don't implement them unless they're trivial.** Note the trade-offs of each:
   - **(a) Copy in `BaseNotifier`.** `__call__`/`__await__` render and notify on `copy.copy(self)` or `copy.deepcopy(self)`. This fixes every notifier, but deepcopy cost/failure on unusual attributes and shallow-copy aliasing of mutable fields both need thought.
   - **(b) Render into locals instead of mutating `self`.** This is a bigger API change, because providers read `self.<field>` in `notify`.
   - **(c) Copy in the dag-processor before invoking callbacks.** This is narrower and misses other reuse paths such as `dag.test()`.

## Deliverable

A short report covering:
- the existing-issue/`main` findings with links
- the reproduction DAG's path and the exact steps and config used
- log excerpts showing `stale=True`
- the failing test(s) and their output
- the fix sketch
- a **draft** GitHub issue (title and body, ready for me to review)

Keep the issue and code generic. Don't mention our company, internal DAG names, hostnames or email addresses.