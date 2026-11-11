# S24 – Online Video Learning (course-video metadata & learner progress)

CS/SS G527 Cloud Computing assignment · AWS · domain concept **data partitioning using consistent hashing**.

```
                         Internet
                            │  (AWS ELB → ingress-nginx)
                    ┌───────▼────────┐  gRPC (round-robin, headless svc)   ┌──────────────────────┐
                    │ 4 content-     │────────────────────────────────────►│ 5 cache-manager x3   │
                    │ delivery-api   │                                      │ StatefulSet, MD5 ring│
                    │ (HPA, public)  │◄─── REST ──┐                         │ 160 vnodes / node,   │
                    └─┬──┬──┬──┬─────┘            │                         │ peer forwarding      │
              REST    │  │  │  │ Kafka            │                         └────────▲─────────────┘
        ┌─────────────┘  │  │  └──────────┐ content-access-events                    │ Kafka
        ▼                ▼  ▼             ▼                                  cache-invalidation
 1 user-service   2 catalog  3 metadata   6 progress-tracker (HPA) ──Kafka──► learner-progress-events
   (RDS PG)       (RDS PG)   (RDS PG+S3)    (DynamoDB LearnerProgress)              │
                                 │ S3 upload (videos/)                              ▼
                                 ▼                                    8 usage-analytics-flink (EMR)
                    7 recommendation-fn (Lambda) ◄── MSK trigger ── usage-analytics-results
                      ├─ LessonPopularity / Recommendations / UsageAnalytics (DynamoDB)
                      └─ failures → SQS DLQ
```

| # | Service | Runtime | Talks via | Storage |
|---|---------|---------|-----------|---------|
| 1 | user-service | EKS Deployment | REST | RDS PostgreSQL `users` |
| 2 | catalog-service | EKS Deployment | REST, Kafka producer | RDS PostgreSQL `catalog` |
| 3 | metadata-service | EKS Deployment | REST, Kafka producer | RDS PostgreSQL `metadata`, S3 |
| 4 | content-delivery-api | EKS Deployment + **HPA**, Ingress | REST, **gRPC** client, Kafka producer | DynamoDB (read) |
| 5 | cache-manager | EKS StatefulSet (3) | **gRPC** server + REST, Kafka consumer | in-memory partitions |
| 6 | progress-tracker | EKS Deployment + **HPA** | REST, Kafka producer | DynamoDB `LearnerProgress` |
| 7 | recommendation-fn | **AWS Lambda** (S3 + MSK triggers) | async events, SQS DLQ | DynamoDB |
| 8 | usage-analytics-flink | **Flink on EMR** | Kafka in/out | Flink state, S3 checkpoints |

## Repository layout

```
terraform/                 all AWS infrastructure (main.tf, variables.tf, outputs.tf, scripts/emr-bootstrap.sh)
services/common/           shared lib (JSON logs, Prometheus RED metrics, Kafka, Postgres, REST client) + cache.proto
services/<service>/        app.py + Dockerfile (build context = services/)
services/recommendation-fn lambda_function.py (zipped & deployed by Terraform)
services/usage-analytics-flink  flink_job.py (uploaded to S3 & submitted to EMR by Terraform) + Dockerfile
k8s-apps/base/             deployments.yaml, hpa.yaml, ingress.yaml, servicemonitor.yaml, Grafana dashboard
k8s-apps/overlays/aws/     generated image pins – the path ArgoCD tracks
argocd-app.yaml            ArgoCD Application (also created by Terraform)
observability/             setup-commands.sh + Helm values (kube-prometheus-stack, Loki, Alloy)
load-test/k6_script.js     sustained load to trigger HPA scale-out
scripts/build-and-push.sh  CI step: build → ECR → update overlay → git push (ArgoCD deploys)
task_division.txt          member / requirement / service split
```

## Prerequisites

AWS account + `aws` CLI (credentials for an admin-ish IAM user), Terraform ≥ 1.6, Docker, kubectl, Helm 3, k6, git, Python 3.
The repository must be on GitHub (public is simplest; for private set `gitops_repo_username/token`).

> Sizing: defaults fit an 8-vCPU EC2 on-demand quota – EKS 2 × t3.large plus a single-node EMR (1 × m5.xlarge), HPAs capped at 6.
> With a larger quota raise `node_*_size` and `emr_core_instance_count` in terraform.tfvars.
> Cost note: EKS, MSK (2 brokers), EMR and RDS run roughly USD 1/hour in ap-south-1. Run `terraform destroy` after the demo.

## Deploy, step by step

```bash
# 0. push this repo to GitHub
git init && git add . && git commit -m "S24 initial" && git branch -M main
git remote add origin https://github.com/<github-user>/s24-video-learning.git && git push -u origin main

# 1. infrastructure (≈ 30–40 min: EKS, MSK and EMR are the slow ones)
cp terraform/terraform.tfvars.example terraform/terraform.tfvars   # set gitops_repo_url
terraform -chdir=terraform init
terraform -chdir=terraform apply
$(terraform -chdir=terraform output -raw kubeconfig_command)

# 2. observability (Prometheus, Grafana, Loki, Alloy)
./observability/setup-commands.sh

# 3. build images, pin them in Git → ArgoCD deploys (no kubectl apply)
./scripts/build-and-push.sh
kubectl -n argocd get applications              # s24-services: Synced / Healthy
kubectl -n s24 get pods,hpa

# 4. ship Flink logs to Loki too (Loki URL printed by step 2), re-creating the EMR cluster
terraform -chdir=terraform apply -var "loki_push_url=http://<internal-lb>/loki/api/v1/push" \
  -replace=aws_emr_cluster.flink

# 5. public URL
export BASE_URL=http://$(kubectl -n ingress-nginx get svc ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].hostname}')
curl $BASE_URL/healthz
```

Browser UI: open `$BASE_URL/ui/` (the Learning Console, served by the content-delivery-api) to create users,
courses and lessons, upload videos, send progress, see cache MISS/HIT with the owning node, and explore the
hash ring, with a log of every API call it makes. `$BASE_URL/docs` is the generated API reference.

Updating a service = change code → `./scripts/build-and-push.sh <service>` → ArgoCD rolls it out.

## Demo script (maps to requirements a–h)

```bash
H='content-type: application/json'
INS=$(curl -s -XPOST $BASE_URL/api/users -H "$H" -d '{"name":"Prof","email":"prof@bits.ac.in","role":"instructor"}' | jq -r .id)
LRN=$(curl -s -XPOST $BASE_URL/api/users -H "$H" -d '{"name":"Asha","email":"asha@bits.ac.in"}' | jq -r .id)
C=$(curl -s -XPOST $BASE_URL/api/courses -H "$H" -H "X-User-Id: $INS" -d '{"title":"Cloud Computing"}' | jq -r .id)
L=$(curl -s -XPOST $BASE_URL/api/courses/$C/lessons -H "$H" -H "X-User-Id: $INS" -d '{"title":"Consistent hashing","duration_seconds":600}' | jq -r .id)
curl -s -XPUT $BASE_URL/api/videos/$L -H "$H" -H "X-User-Id: $INS" -d '{"title":"CH lecture","duration_seconds":600}'
curl -s -XPOST $BASE_URL/api/videos/$L/upload -H "X-User-Id: $INS" -F "file=@lecture.mp4;type=video/mp4"   # (g) S3 → (b) Lambda

# (f) consistent hashing + cache-aside: 1st read MISS, 2nd HIT; owner = cache-manager-N
curl -s $BASE_URL/api/lessons/$L -H "X-User-Id: $LRN" | jq .cache
curl -s $BASE_URL/api/lessons/$L -H "X-User-Id: $LRN" | jq .cache
curl -s "$BASE_URL/api/cache/lookup?key=lesson:$L" | jq
curl -s $BASE_URL/api/cache/ring | jq .ownership
curl -s "$BASE_URL/api/cache/simulate?add=cache-manager-3" | jq '.consistent_hashing,.modulo_hashing'   # ~25% vs ~75%
curl -s "$BASE_URL/api/cache/simulate?add=cache-manager-3&vnodes=1" | jq .keys_per_node_after           # skew without vnodes
# invalidation: update the lesson → Kafka cache-invalidation → owner drops the key → next read MISS
curl -s -XPUT $BASE_URL/api/lessons/$L -H "$H" -H "X-User-Id: $INS" -d '{"title":"Consistent hashing (v2)"}'
curl -s $BASE_URL/api/lessons/$L -H "X-User-Id: $LRN" | jq '.lesson.title,.cache.lesson'

# progress → DynamoDB + Kafka → Flink (1-min window) → Kafka → Lambda → DynamoDB
curl -s -XPOST $BASE_URL/api/progress -H "$H" -H "X-User-Id: $LRN" \
  -d "{\"lesson_id\":\"$L\",\"course_id\":\"$C\",\"event\":\"play\",\"position_seconds\":0,\"duration_seconds\":600}"
sleep 90; curl -s $BASE_URL/api/analytics/items/$L -H "X-User-Id: $INS" | jq .totals            # (e)
curl -s $BASE_URL/api/users/$LRN/recommendations -H "X-User-Id: $LRN" | jq                        # (b)

# (c) autoscaling under load
k6 run -e BASE_URL=$BASE_URL load-test/k6_script.js &
kubectl -n s24 get hpa -w
```

Grafana → dashboard **S24 Online Video Learning – Overview** (RPS, error rate, p50/p95/p99, HPA replicas, node health, cache, Kafka, Loki logs incl. `namespace="emr"` for the Flink job).

## How the core concept is implemented

* `services/cache-manager/app.py` §1 – `ConsistentHashRing`: MD5 (first 64 bits), 160 virtual nodes per physical node, `bisect` lookup O(log V), preference lists, ownership analysis, rebalance simulation vs. modulo hashing.
* Physical nodes are StatefulSet pods `cache-manager-{0..2}` (stable DNS through the headless service). Any replica accepts a gRPC call and forwards it to the owner, so clients stay partition-agnostic.
* Cache-aside lives in the Content Delivery API: `Get` → on miss read source (catalog/metadata over REST) → `Set` with TTL and `read_started_ms`.
* Expiry: per-key TTL, lazy expiry on read, background sweeper, LRU bound.
* Invalidation: catalog/metadata publish `cache-invalidation`; every replica consumes (unique group id), owners drop keys and keep a 10 s tombstone that rejects Sets whose read began before the invalidation (cache-aside race).
* Stale reads: 10 % of cache hits are re-validated against the source in the background; mismatched versions are counted, emitted to Kafka, repaired, and summarised per minute by Flink (`stale_reads`).

## Local smoke test without AWS

The Python services run against any PostgreSQL plus an AWS mock (e.g. `moto_server`) with
`AWS_ENDPOINT_URL`, `AWS_ENDPOINT_URL_S3`, `DB_SSLMODE=disable`. With `KAFKA_BOOTSTRAP_SERVERS` unset the
services log that Kafka is disabled and keep working. Cache replicas can run on one machine with
`CACHE_PEER_ADDRESSES=localhost:50061,localhost:50062,localhost:50063` and `CACHE_SELF_ADDRESS=<own>`.

## Teardown

The observability stack is installed with Helm outside Terraform; its load balancers would block
deleting the VPC, so remove it first.

```bash
helm uninstall alloy loki kube-prometheus-stack -n monitoring --wait
kubectl delete namespace monitoring
terraform -chdir=terraform destroy
```
