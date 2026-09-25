
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


with DAG(
    dag_id="fmcg_sales_pipeline",
    start_date=datetime(2026, 9, 13),
    schedule=None,
    catchup=False,
    tags=["FMCG", "Data Engineering"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    kafka_ingestion = DockerOperator(
        task_id="kafka_ingestion",
        image="fmcg-kafka-ingestion",
        command=[
            "python",
            "/app/consumer_hdfs.py",
        ],
        docker_url="unix://var/run/docker.sock",
        mounts=[
            Mount(
                source="/var/run/docker.sock",
                target="/var/run/docker.sock",
                type="bind",
            )
        ],
        auto_remove="success",
        mount_tmp_dir=False,
        network_mode="docker_default",
    )

    spark_etl = DockerOperator(
        task_id="spark_etl",
        image="docker:cli",
        command=[
            "docker",
            "exec",
            "fmcg-spark",
            "/opt/spark/bin/spark-submit",
            "/opt/spark/jobs/fmcg_etl.py",
        ],
        docker_url="unix://var/run/docker.sock",
        mounts=[
            Mount(
                source="/var/run/docker.sock",
                target="/var/run/docker.sock",
                type="bind",
            )
        ],
        auto_remove="success",
        mount_tmp_dir=False,
        network_mode="docker_default",
    )

    data_quality_check = DockerOperator(
        task_id="data_quality_check",
        image="docker:cli",
        command=[
            "docker",
            "exec",
            "fmcg-spark",
            "/opt/spark/bin/spark-submit",
            "/opt/spark/jobs/data_quality_check.py",
        ],
        docker_url="unix://var/run/docker.sock",
        mounts=[
            Mount(
                source="/var/run/docker.sock",
                target="/var/run/docker.sock",
                type="bind",
            )
        ],
        auto_remove="success",
        mount_tmp_dir=False,
        network_mode="docker_default",
    )

    hive_analytics = DockerOperator(
        task_id="hive_analytics",
        image="docker:cli",
        command=[
            "docker",
            "exec",
            "-i",
            "-e",
            "HADOOP_CLIENT_OPTS=-Dorg.jline.terminal.provider=dumb",
            "fmcg-hive",
            "beeline",
            "-u",
            "jdbc:hive2://localhost:10000/fmcg",
            "-e",
            "SELECT country, ROUND(SUM(net_sales),2) AS total_sales, "
            "SUM(units_sold) AS total_units "
            "FROM sales "
            "GROUP BY country "
            "ORDER BY total_sales DESC;",
        ],
        docker_url="unix://var/run/docker.sock",
        mounts=[
            Mount(
                source="/var/run/docker.sock",
                target="/var/run/docker.sock",
                type="bind",
            )
        ],
        tty=True,
        auto_remove="success",
        mount_tmp_dir=False,
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> kafka_ingestion >> spark_etl >> data_quality_check >> hive_analytics >> end