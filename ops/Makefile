.PHONY: up up-external down test test-log reissue-key create-migration logs seed

up: ## Start with local PostgreSQL (plug & play)
	docker-compose up -d
	@echo "Started with local PostgreSQL (http://localhost:8000)"

up-external: ## Start with external DB (requires DATABASE_URL in .env)
	docker-compose --profile external up -d backend-external
	@echo "Started with external DATABASE_URL"

down:
	docker-compose down
	docker-compose --profile external down 2>/dev/null || true

logs:
	docker-compose logs -f backend

test: ## Run tests
	docker exec ops-backend-1 python -m pytest tests/ -v

test-log: ## Run tests (save to file)
	docker exec ops-backend-1 sh -c "python -m pytest tests/ -v --tb=long 2>&1 | tee /app/test_results.log"
	@echo "Results saved to: backend/test_results.log"

reissue-key:
	docker exec -w /app ops-backend-1 python utils/reissue_dev_key.py

create-migration: ## Create Alembic migration (usage: make create-migration MESSAGE="description")
	docker exec -w /app ops-backend-1 alembic revision --autogenerate -m "$(MESSAGE)"

seed: ## Run seed scripts
	docker exec -w /app ops-backend-1 sh -c "python utils/seed_slack_template.py && python utils/seed_linear_template.py && python utils/seed_tests.py"
