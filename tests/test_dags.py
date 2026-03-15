"""
Tests for Airflow DAG validation.

These tests ensure DAGs load correctly without import errors
and have proper structure and dependencies.
"""

import pytest
from pathlib import Path


# Add new DAGs here when created - test will fail if you forget!
REGISTERED_DAGS = [
    "flight_dag.py",
    "bank_churn_dag.py",
    "alpha_market_data.py",
    "transport_etl.py",
]


class TestDAGIntegrity:
    """Test DAG files for basic integrity and structure."""

    def test_all_dags_are_registered(self):
        """Fail if a new DAG file exists but isn't registered for testing."""
        dag_dir = Path(__file__).parent.parent / "dags"
        
        actual_dags = {
            f.name for f in dag_dir.glob("*.py") 
            if not f.name.startswith("__")
        }
        registered = set(REGISTERED_DAGS)
        
        unregistered = actual_dags - registered
        if unregistered:
            pytest.fail(
                f"New DAG file(s) found without test coverage: {unregistered}. "
                f"Add them to REGISTERED_DAGS in test_dags.py and write tests!"
            )

    def test_dag_files_exist(self):
        """Verify all registered DAG files exist."""
        dag_dir = Path(__file__).parent.parent / "dags"
        
        for dag_file in REGISTERED_DAGS:
            assert (dag_dir / dag_file).exists(), f"DAG file {dag_file} not found"

    def test_dag_files_have_no_syntax_errors(self):
        """Check DAG files for Python syntax errors."""
        dag_dir = Path(__file__).parent.parent / "dags"
        
        for dag_file in dag_dir.glob("*.py"):
            if dag_file.name.startswith("__"):
                continue
            
            with open(dag_file) as f:
                source = f.read()
            
            try:
                compile(source, dag_file, "exec")
            except SyntaxError as e:
                pytest.fail(f"Syntax error in {dag_file.name}: {e}")


class TestDAGImports:
    """Test DAG imports (requires Airflow to be installed)."""

    @pytest.fixture(autouse=True)
    def skip_if_no_airflow(self):
        """Skip tests if Airflow is not installed."""
        pytest.importorskip("airflow")

    def test_flight_dag_loads(self):
        """Test that flight_dag.py loads without errors."""
        from dags.flight_dag import dag
        
        assert dag is not None
        assert dag.dag_id == "flight_details_etl"

    def test_flight_dag_has_correct_tasks(self):
        """Verify flight DAG has expected tasks."""
        from dags.flight_dag import dag
        
        task_ids = [task.task_id for task in dag.tasks]
        
        assert "bronze_ingest" in task_ids
        assert "silver_transform" in task_ids
        assert "gold_aggregate" in task_ids

    def test_flight_dag_task_dependencies(self):
        """Verify flight DAG task dependencies are correct."""
        from dags.flight_dag import dag
        
        # Get tasks
        bronze = dag.get_task("bronze_ingest")
        silver = dag.get_task("silver_transform")
        gold = dag.get_task("gold_aggregate")
        
        # Check dependencies: bronze >> silver >> gold
        assert silver.task_id in [t.task_id for t in bronze.downstream_list]
        assert gold.task_id in [t.task_id for t in silver.downstream_list]

    def test_dag_default_args(self):
        """Verify DAG has proper default arguments."""
        from dags.flight_dag import dag
        
        assert dag.default_args.get("owner") is not None
        assert dag.default_args.get("retries") is not None


class TestDAGConfiguration:
    """Test DAG configuration and scheduling."""

    @pytest.fixture(autouse=True)
    def skip_if_no_airflow(self):
        """Skip tests if Airflow is not installed."""
        pytest.importorskip("airflow")

    def test_flight_dag_schedule(self):
        """Verify flight DAG has correct schedule."""
        from dags.flight_dag import dag
        
        # Airflow 2.4+ uses schedule instead of schedule_interval
        schedule = getattr(dag, 'schedule', None) or getattr(dag, 'schedule_interval', None)
        assert schedule is not None

    def test_flight_dag_catchup_disabled(self):
        """Verify catchup is disabled to prevent backfill storms."""
        from dags.flight_dag import dag
        
        assert dag.catchup is False

    def test_dag_has_tags(self):
        """Verify DAG has tags for organization."""
        from dags.flight_dag import dag
        
        assert dag.tags is not None
        assert len(dag.tags) > 0
