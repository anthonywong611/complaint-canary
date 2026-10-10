# Common tasks. Cloud targets take ENV=dev|qa|prod and TF_STATE_BUCKET=<bucket>.
ENV ?= dev
TF := terraform -chdir=terraform
TF_VARS := -var-file=envs/$(ENV).tfvars -var-file=envs/$(ENV).local.tfvars

.PHONY: setup fixture run day2 status ui test lint format docs docker up down \
        tf-init tf-plan tf-apply tf-destroy tf-check ui-tunnel

setup:  ## Install dependencies and create .env
	uv sync
	test -f .env || cp .env.example .env

fixture:  ## Write the synthetic fixture to data/fixture
	uv run canary fixture

run:  ## Run the daily job on the day-1 fixture
	uv run canary run

day2:  ## Run the daily job on the day-2 fixture
	CANARY_CFPB_URL=data/fixture/day2/complaints.csv.zip uv run canary run

status:  ## Show the snapshot ledger
	uv run canary status

ui:  ## Dagster UI on http://localhost:3000
	uv run dagster dev

test:
	uv run pytest

lint:
	uv run ruff format --check .
	uv run ruff check .
	uv run sqlfluff lint dbt

format:
	uv run ruff format .
	uv run ruff check --fix .

docs:  ## dbt docs from the local warehouse, in ./site
	CANARY_DUCKDB_PATH=$(CURDIR)/data/warehouse.duckdb uv run dbt docs generate \
		--project-dir dbt --profiles-dir dbt --target local --target-path $(CURDIR)/site \
		--vars '{"snapshot_date": "2026-09-30"}'

docker:
	docker build -t complaint-canary:local .

up:  ## Local stack in Docker (needs .env)
	docker compose up -d --build webserver daemon

down:
	docker compose down

tf-check:
	$(TF) fmt -check -recursive
	$(TF) init -backend=false -input=false
	$(TF) validate

tf-init:
	$(TF) init -reconfigure -backend-config="bucket=$(TF_STATE_BUCKET)" \
		-backend-config="prefix=complaint-canary/$(ENV)"

tf-plan: tf-init
	$(TF) plan $(TF_VARS)

tf-apply: tf-init
	$(TF) apply $(TF_VARS)

tf-destroy: tf-init
	$(TF) destroy $(TF_VARS)

ui-tunnel:  ## Forward the cloud Dagster UI to http://localhost:3000
	gcloud compute ssh canary-$(ENV) --tunnel-through-iap -- -N -L 3000:localhost:3000
