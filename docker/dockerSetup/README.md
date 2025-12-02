Kræver docker
Samt denne kommando 
docker plugin install grafana/loki-docker-driver:3.3.2-amd64 --alias loki --grant-all-permissions
(-arm64 i stedet for amd64 hvis arm64 vært)
Og dette skal i /etc/docker/daemon.json
{
    "log-driver": "loki",
    "log-opts": {
      "loki-url": "http://localhost:3100/loki/api/v1/push",
      "loki-batch-size": "400"
      "loki-retries": "5",
      "loki-min-backoff": "1s",
      "loki-max-backoff": "30s"
    }
}

Yderligere kræves der opsætning af 3 netværk
docker network create api_net
docker network create loki_net
docker network create mqtt_net