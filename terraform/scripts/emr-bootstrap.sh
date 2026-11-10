#!/bin/bash
# EMR bootstrap action (runs on every node before applications start).
#   $1 = apache-flink (PyFlink) pip version     e.g. 1.18.1
#   $2 = flink-sql-connector-kafka version      e.g. 3.2.0-1.18
#   $3 = Loki push URL or "none"                e.g. http://internal-xxx.elb.amazonaws.com/loki/api/v1/push
set -euxo pipefail

PYFLINK_VERSION="${1:-1.18.1}"
KAFKA_CONNECTOR_VERSION="${2:-3.2.0-1.18}"
LOKI_URL="${3:-none}"

# ---------------------------------------------------------------- PyFlink runtime
sudo dnf install -y python3-pip python3-devel gcc
# AL2023 ships pip as an RPM, which pip cannot uninstall, so install alongside it.
sudo python3 -m pip install --upgrade --ignore-installed "pip<25"
sudo python3 -m pip install "apache-flink==${PYFLINK_VERSION}"

# ---------------------------------------------------------------- Kafka SQL connector
# Downloaded to a fixed path; flink_job.py registers it through `pipeline.jars`,
# so Flink ships it to the JobManager/TaskManager containers on YARN.
sudo mkdir -p /opt/s24
sudo curl -fsSL -o /opt/s24/flink-sql-connector-kafka.jar \
  "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar"
sudo chmod 644 /opt/s24/flink-sql-connector-kafka.jar

# ---------------------------------------------------------------- Log shipping to Loki (Grafana Alloy)
if [ "${LOKI_URL}" != "none" ]; then
  sudo tee /etc/yum.repos.d/grafana.repo >/dev/null <<'EOF'
[grafana]
name=grafana
baseurl=https://rpm.grafana.com
repo_gpgcheck=1
enabled=1
gpgcheck=1
gpgkey=https://rpm.grafana.com/gpg.key
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
EOF
  sudo dnf install -y alloy

  NODE_NAME="$(hostname)"
  sudo tee /etc/alloy/config.alloy >/dev/null <<EOF
local.file_match "flink" {
  path_targets = [
    {"__path__" = "/var/log/hadoop-yarn/containers/*/*/*.log", "job" = "usage-analytics-flink", "service" = "usage-analytics-flink", "node" = "${NODE_NAME}", "namespace" = "emr"},
    {"__path__" = "/var/log/flink/*.log", "job" = "flink-system", "service" = "usage-analytics-flink", "node" = "${NODE_NAME}", "namespace" = "emr"},
  ]
  sync_period = "15s"
}

loki.source.file "flink" {
  targets    = local.file_match.flink.targets
  forward_to = [loki.write.default.receiver]
}

loki.write "default" {
  endpoint {
    url = "${LOKI_URL}"
  }
}
EOF
  # YARN container logs are only readable by the yarn group / root.
  sudo mkdir -p /etc/systemd/system/alloy.service.d
  sudo tee /etc/systemd/system/alloy.service.d/override.conf >/dev/null <<'EOF'
[Service]
User=root
EOF
  sudo systemctl daemon-reload
  sudo systemctl enable --now alloy
fi
