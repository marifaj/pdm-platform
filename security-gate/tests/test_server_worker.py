"""The queue and the worker: the boundary between the UI and scanner execution."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from markna_server.domain import RunStatus, Verdict
from markna_server.execution import RunExecutor, Worker
from conftest import make_user, principal_for


@pytest.fixture
def worker(services, config) -> Worker:
    return Worker(services.uow, config, worker_id="worker-a")


class TestQueueSemantics:
    def test_claiming_moves_a_run_to_running(self, services, config, admin, project):
        run = services.runs.enqueue(admin, project.id, layers=["architecture"])
        claimed = services.uow.runs.claim_next_queued("worker-a")
        assert claimed is not None and claimed.id == run.id
        assert claimed.status is RunStatus.RUNNING
        assert claimed.worker_id == "worker-a"
        assert claimed.started_at is not None

    def test_an_empty_queue_returns_nothing(self, services):
        assert services.uow.runs.claim_next_queued("worker-a") is None

    def test_only_one_worker_can_claim_a_run(self, services, config, admin, project):
        services.runs.enqueue(admin, project.id, layers=["architecture"])
        first = services.uow.runs.claim_next_queued("worker-a")
        second = services.uow.runs.claim_next_queued("worker-b")
        assert first is not None
        assert second is None, "two workers claimed the same run"

    def test_two_workers_split_two_runs(self, services, config, admin, project):
        services.runs.enqueue(admin, project.id, layers=["architecture"])
        services.runs.enqueue(admin, project.id, layers=["architecture"])
        first = services.uow.runs.claim_next_queued("worker-a")
        second = services.uow.runs.claim_next_queued("worker-b")
        assert first and second and first.id != second.id

    def test_the_queue_is_first_in_first_out(self, services, config, admin, project):
        queued = [
            services.runs.enqueue(admin, project.id, layers=["architecture"]) for _ in range(3)
        ]
        claimed = [services.uow.runs.claim_next_queued("w") for _ in range(3)]
        assert [run.id for run in claimed] == [run.id for run in queued]

    def test_a_cancelled_run_is_never_claimed(self, services, config, admin, project):
        run = services.runs.enqueue(admin, project.id, layers=["architecture"])
        assert services.runs.cancel(admin, run.id)
        assert services.uow.runs.claim_next_queued("worker-a") is None

    def test_a_running_run_cannot_be_cancelled(self, services, admin, project):
        run = services.runs.enqueue(admin, project.id, layers=["architecture"])
        services.uow.runs.claim_next_queued("worker-a")
        assert services.runs.cancel(admin, run.id) is False


class TestExecution:
    def test_a_run_produces_findings_coverage_and_reports(
        self, services, config, worker, admin, project
    ):
        run = services.runs.enqueue(admin, project.id, layers=["architecture"])
        assert worker.run_once() is not None

        scope = admin.scope
        stored = services.uow.runs.get(scope, run.id)
        assert stored.status is RunStatus.SUCCEEDED
        assert stored.verdict is Verdict.BLOCK
        assert stored.finished_at is not None
        assert services.uow.findings.count_for_run(scope, run.id) > 0
        assert services.uow.run_details.coverage_for(scope, run.id)
        assert services.uow.run_details.scanner_runs_for(scope, run.id)
        assert {report.format for report in services.uow.reports.list_for_run(scope, run.id)} == {
            "json",
            "markdown",
        }

    def test_persisted_findings_carry_the_owning_tenant_and_project(
        self, services, worker, admin, project
    ):
        run = services.runs.enqueue(admin, project.id, layers=["architecture"])
        worker.run_once()
        findings = services.uow.findings.list_for_run(admin.scope, run.id)
        assert findings
        assert all(finding.organization_id == admin.organization_id for finding in findings)
        assert all(finding.project_id == project.id for finding in findings)

    def test_findings_keep_the_engine_identifier(self, services, worker, admin, project):
        run = services.runs.enqueue(admin, project.id, layers=["architecture"])
        worker.run_once()
        findings = services.uow.findings.list_for_run(admin.scope, run.id)
        assert all(finding.engine_id.startswith("MK-") for finding in findings)
        assert all(finding.fingerprint for finding in findings)

    def test_the_summary_matches_the_stored_findings(self, services, worker, admin, project):
        run = services.runs.enqueue(admin, project.id, layers=["architecture"])
        worker.run_once()
        stored = services.uow.runs.get(admin.scope, run.id)
        assert stored.summary["total_findings"] == services.uow.findings.count_for_run(
            admin.scope, run.id
        )

    def test_drain_executes_everything_queued(self, services, worker, admin, project):
        for _ in range(3):
            services.runs.enqueue(admin, project.id, layers=["architecture"])
        assert worker.drain() == 3
        assert worker.drain() == 0

    def test_scratch_files_are_cleaned_up(self, services, config, worker, admin, project):
        run = services.runs.enqueue(admin, project.id, layers=["architecture"])
        worker.run_once()
        assert not (Path(config.worker_workdir) / run.id).exists()


class TestFailureHandling:
    def test_a_missing_source_path_fails_the_run_not_the_worker(
        self, services, config, worker, admin, project
    ):
        shutil.rmtree(Path(config.workspace_root) / "demo")
        run = services.runs.enqueue(admin, project.id, layers=["code"])
        completed = worker.run_once()

        assert completed.status is RunStatus.FAILED
        assert "does not exist" in completed.error
        assert services.uow.runs.get(admin.scope, run.id).status is RunStatus.FAILED

    def test_deleting_a_project_takes_its_runs_with_it(self, services, admin, project):
        """Runs cascade from their project, so there is no orphan to execute."""
        run = services.runs.enqueue(admin, project.id, layers=["architecture"])
        services.uow.db.execute("DELETE FROM projects WHERE id=?", (project.id,))
        assert services.uow.runs.get(admin.scope, run.id) is None
        assert services.uow.runs.claim_next_queued("worker-a") is None

    def test_a_deleted_target_fails_the_run_cleanly(self, services, worker, admin, project):
        target = services.projects.add_target(
            admin, project.id, name="uat", url="https://uat.example.com", authorized_by="me"
        )
        run = services.runs.enqueue(admin, project.id, layers=["environment"])
        services.projects.delete_target(admin, target.id)

        completed = worker.run_once()
        assert completed.status is RunStatus.FAILED
        assert "target" in completed.error
        assert services.uow.runs.get(admin.scope, run.id).status is RunStatus.FAILED

    def test_an_engine_exception_is_recorded_on_the_run(
        self, services, config, admin, project, monkeypatch
    ):
        services.runs.enqueue(admin, project.id, layers=["architecture"])

        class ExplodingExecutor(RunExecutor):
            def _build_run_config(self, run, inputs):
                config = super()._build_run_config(run, inputs)
                monkeypatch.setattr(
                    "markna_server.execution.adapter.Runner",
                    lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
                )
                return config

        worker = Worker(services.uow, config, executor=ExplodingExecutor(services.uow, config))
        completed = worker.run_once()
        assert completed.status is RunStatus.FAILED
        assert "boom" in completed.error

    def test_the_worker_loop_survives_a_failing_run(self, services, config, admin, project):
        services.runs.enqueue(admin, project.id, layers=["architecture"])

        class BrokenExecutor(RunExecutor):
            def execute(self, run):
                raise RuntimeError("executor exploded")

        worker = Worker(services.uow, config, executor=BrokenExecutor(services.uow, config))
        with pytest.raises(RuntimeError):
            worker.run_once()
        # run_forever swallows it; the queue entry stays claimed and visible.
        stored = services.uow.runs.list(admin.scope)[0]
        assert stored.status is RunStatus.RUNNING


class TestWorkerTenancy:
    def test_the_worker_writes_into_the_runs_own_organization(self, services, config, admin, project):
        other = services.organizations.bootstrap("other", "Other")
        other_admin = principal_for(make_user(services, other, email="a@other"))
        other_project = services.projects.create(
            other_admin,
            slug="other-demo",
            name="Other Demo",
            architecture_manifest="demo/architecture.yaml",
        )
        services.runs.enqueue(admin, project.id, layers=["architecture"])
        other_run = services.runs.enqueue(other_admin, other_project.id, layers=["architecture"])

        Worker(services.uow, config, worker_id="shared").drain()

        # Each organisation sees only its own results, from one shared worker.
        assert services.uow.findings.count_for_run(other_admin.scope, other_run.id) > 0
        assert services.uow.findings.list_for_run(admin.scope, other_run.id) == []
