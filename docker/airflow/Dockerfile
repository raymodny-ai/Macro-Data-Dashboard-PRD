FROM apache/airflow:2.8.1-python3.11

# 切换到 root 安装系统依赖
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 切换回 airflow 用户
USER airflow

# 安装 ETL 依赖
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# 复制 DAG 文件
COPY dags/ /opt/airflow/dags/

# 设置环境变量
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=false
ENV AIRFLOW__CORE__CATCHUP_BY_DEFAULT=false
