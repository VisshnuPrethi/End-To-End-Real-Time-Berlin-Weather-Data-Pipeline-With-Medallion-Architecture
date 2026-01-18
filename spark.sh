# Added by master_install_spark_jupyter.sh
export SPARK_HOME="/opt/spark"
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
# Fixed Hadoop defaults
export HADOOP_HOME="/usr/local/hadoop"
export HADOOP_CONF_DIR="/usr/local/hadoop/etc/hadoop"
# Executors' Python (must exist on all nodes)
export PYSPARK_PYTHON="/usr/bin/python3.12"
