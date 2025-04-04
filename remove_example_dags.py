from airflow.models.dag import DagModel
from airflow.models.serialized_dag import SerializedDagModel
from airflow import settings


session = settings.Session()

# Step 1: Delete from SerializedDagModel
serialized_dags = session.query(SerializedDagModel).filter(SerializedDagModel.dag_id.like("example_%")).all()
for dag in serialized_dags:
    print(f"Deleting serialized DAG: {dag.dag_id}")
    session.delete(dag)

# Step 2: Delete from DagModel
example_dags = session.query(DagModel).filter(DagModel.dag_id.like("example_%")).all()
for dag in example_dags:
    print(f"Deleting DAG: {dag.dag_id}")
    session.delete(dag)

session.commit()
print("✅ All example DAGs removed.")
