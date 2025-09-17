# CoMaKo Deployment Checklist

This checklist ensures a smooth and secure deployment of the CoMaKo platform.

## Phase 1: Pre-Deployment

- [ ] **Infrastructure Provisioning**:
  - [ ] Server/VM meets minimum requirements (e.g., 2 vCPU, 4GB RAM, 20GB disk space).
  - [ ] Docker and Docker Compose are installed and running.
  - [ ] Firewall rules are configured to allow traffic on required ports (e.g., 80, 443, 21, 5672).

- [ ] **Configuration Management**:
  - [ ] A production `.env` file has been created and populated with secure credentials.
  - [ ] `DATABASE_URL` points to the production PostgreSQL instance.
  - [ ] `RABBITMQ_URL` points to the production RabbitMQ instance.
  - [ ] All secrets (passwords, API keys) have been generated securely and are not hard-coded.
  - [ ] Production certificates for AS2/AS4 are in place and paths are correctly set in `.env`.

- [ ] **Code & Dependencies**:
  - [ ] The correct Git branch (e.g., `main` or a release tag) is checked out.
  - [ ] Python dependencies are installed via `pip install -r requirements.txt`.

## Phase 2: Deployment

- [ ] **Start Services**:
  - [ ] Run `docker compose -f docker-compose.prod.yml up -d` (assuming a production-specific compose file).
  - [ ] Verify all containers (db, rabbitmq, ftp) are running without errors using `docker ps`.

- [ ] **Database Migration**:
  - [ ] Apply all pending database migrations: `alembic upgrade head`.
  - [ ] Verify the schema version is correct: `alembic current`.

- [ ] **Application Startup**:
  - [ ] Start the FastAPI application using a production-grade server like Gunicorn with Uvicorn workers.
    ```bash
    gunicorn -w 4 -k uvicorn.workers.UvicornWorker src.main:app
    ```

## Phase 3: Post-Deployment Verification

- [ ] **Health Checks**:
  - [ ] Access the health check endpoint (`/health`) and confirm a successful response (`{"status":"ok","db":"connected"}`).
  - [ ] Check RabbitMQ management UI to ensure queues and exchanges are correctly set up.
  - [ ] Test FTP server connectivity.

- [ ] **Functionality Tests**:
  - [ ] Run essential end-to-end tests or smoke tests against the live system.
  - [ ] Submit a test meter reading and verify it is processed correctly.
  - [ ] Upload a sample EDI file and check for the expected APERAK response.

- [ ] **Security Checks**:
  - [ ] Verify that API documentation (`/api/docs`) is disabled or protected in the production environment.
  - [ ] Ensure all external-facing endpoints have proper authentication and authorization controls.
  - [ ] Confirm that logging levels are set appropriately for production (e.g., `INFO` or `WARNING`).

- [ ] **Monitoring**:
  - [ ] Check that monitoring tools (e.g., Prometheus, Grafana) are receiving metrics from the application.
  - [ ] Verify that logs are being collected and are accessible.
