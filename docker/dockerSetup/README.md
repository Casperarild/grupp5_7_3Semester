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
    }
}
