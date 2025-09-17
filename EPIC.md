# EPIC Implementation Plan for CoMaKo

## Phase 1: Core Infrastructure Setup
- [x] **1. Docker Infrastructure Setup**  
  Create `docker-compose.yml` with:  
  ```yaml
  services:
    db:
      image: postgres:15
      environment:
        POSTGRES_DB: comako
        POSTGRES_USER: comako
        POSTGRES_PASSWORD: comako
      ports:
        - "5432:5432"
      volumes:
        - pgdata:/var/lib/postgresql/data
  
  volumes:
    pgdata:
  ```

- [x] **2. FastAPI Project Scaffolding**  
  Create directory structure:  
  ```
  src/
  ├── main.py
  ├── config.py
  ├── models/
  │   └── __init__.py
  ├── services/
  └── alembic/
  ```

- [x] **3. SQLAlchemy Model Implementation**  
  Translate `db_schema.md` into ORM models in `src/models/` with:  
  - `MarketRole` (Enum)
  - `MarketParticipant` with relationship to `ParticipantRoles`
  - `MeteringPoint` with `SupplyContracts` relationship
  - `BalanceGroup` with `BalanceGroupMembers` association
  - `EnergyFlow` and `EnergyReading` models
  - `SettlementRun` model

- [x] **4. Async Database Configuration**  
  Implement in `src/config.py`:  
  ```python
  from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
  
  engine = create_async_engine(
      "postgresql+asyncpg://comako:comako@db:5432/comako",
      echo=True
  )
  async_session = async_sessionmaker(engine, expire_on_commit=False)
  ```

- [x] **5. Alembic Migration Setup**  
  Execute:  
  ```bash
  alembic init alembic
  ```  
  Configure `alembic.ini` and `alembic/env.py` to use async models, then generate initial migration:  
  ```bash
  alembic revision --autogenerate -m "initial schema"
  ```

- [x] **6. Health Check Endpoint**  
  Add to `src/main.py`:  
  ```python
  @app.get("/health")
  async def health_check():
      async with async_session() as session:
          await session.execute("SELECT 1")
          return {"status": "ok", "db": "connected"}
  ```

- [x] **7. Verification Protocol**  
  Confirm runnable system by:  
  1. Starting containers: `docker compose up -d`  
  2. Running migrations: `alembic upgrade head`  
  3. Starting dev server: `uvicorn src.main:app --reload`  
  4. Validating: `curl http://localhost:8000/health` → `{"status":"ok","db":"connected"}`

## Phase 2: Market Core Implementation
- [x] **1. BalanceGroup Repository Implementation**  
  Create `src/services/balance_group.py` with:  
  - `create_balance_group()`
  - `get_balance_group()`
  - `add_member()`
  - `remove_member()`
  - Unit tests with pytest

- [x] **2. Real-time Aggregation Engine**  
  Implement in `src/services/energy_flow.py`:  
  ```python
  async def aggregate_energy_flows(balance_group_id: str):
      # Query EnergyReading records
      # Sum consumption/generation
      # Return aggregated values
  ```

- [x] **3. Deviation Calculation Module**  
  Create `src/services/deviation.py`:  
  ```python
  def calculate_deviation(actual: float, forecast: float) -> float:
      return actual - forecast
  ```

- [x] **4. Settlement Logic Implementation**  
  In `src/services/settlement.py`:  
  ```python
  def calculate_settlement(deviation_kwh: float, price_ct_per_kwh: int = 10) -> float:
      return (deviation_kwh * price_ct_per_kwh) / 100  # Convert ct to EUR
  ```

- [x] **5. Settlement Report Endpoint**  
  Add to `src/main.py`:  
  ```python
  @app.get("/balance_groups/{id}/report")
  async def generate_report(id: str):
      # Fetch settlement data
      # Format as JSON response
  ```

- [x] **6. Verification Protocol**  
  Confirm functionality by:  
  1. Creating a balance group with members  
  2. Submitting test energy readings  
  3. Running settlement calculation  
  4. Validating report output matches expected format

- [x] **7. Unit Test Suite**  
  Create comprehensive unit tests with pytest:  
  1. `tests/unit/test_energy_flow.py` - Energy aggregation tests  
  2. `tests/unit/test_settlement.py` - Settlement calculation tests  
  3. `tests/unit/test_deviation.py` - Deviation calculation tests  
  4. Coverage target: >90% for all Phase 2 modules

## Phase 3: Meter Gateway Development
- [x] **1. POST /readings Endpoint Implementation**  
  Create `src/services/meter_reading.py` with:  
  - `submit_reading()` handling CSV/voicebot input  
  - Validation logic for data format  
  - Unit tests with pytest  

- [x] **2. GET /readings/{id} Endpoint**  
  Implement in `src/main.py`:  
  ```python
  @app.get("/readings/{id}")
  async def get_reading(id: str):
      # Fetch reading from database
      # Return JSON response
  ```

- [x] **3. GET /readings/anomalies Endpoint**  
  Build outlier detection in `src/services/anomaly_detection.py`:  
  ```python
  def detect_anomalies(readings: list) -> list:
      # Statistical analysis to identify outliers
      return [r for r in readings if is_outlier(r)]
  ```

- [x] **4. Validation Pipeline Development**  
  Create Pydantic models in `src/models/meter_reading.py`:  
  ```python
  class MeterReading(BaseModel):
      metering_point: str
      timestamp: datetime
      value_kwh: float
      source: str  # "CSV", "voicebot", etc.
  ```

- [x] **5. Market Core Integration**  
  Implement internal API client in `src/clients/market_core.py`:  
  ```python
  async def send_to_market_core(reading: dict):
      async with httpx.AsyncClient() as client:
          response = await client.post(
              "http://market_core/settlement",
              json=reading
          )
          return response.json()
  ```

- [x] **6. Verification Protocol**  
  Confirm functionality by:  
  1. Submitting test reading via POST /readings  
  2. Retrieving it via GET /readings/{id}  
  3. Checking anomaly detection with synthetic outliers  
  4. Validating data flow to Market Core

- [x] **7. Unit Test Suite**  
  Create comprehensive unit tests with pytest:  
  1. `tests/unit/test_meter_reading.py` - Meter reading repository tests  
  2. `tests/unit/test_anomaly_detection.py` - Anomaly detection tests  
  3. `tests/unit/test_market_core_client.py` - Market Core client tests  
  4. `tests/unit/test_pydantic_models.py` - Validation model tests  
  5. Coverage target: >90% for all Phase 3 modules

## Phase 4: EDI Gateway Implementation
- [x] **1. EDIFACT Parser Implementation**  
  Create `src/services/edi_parser.py` with:  
  - `parse_edi_file()` handling UTILMD/MSCONS  
  - Segment validation logic  
  - Unit tests with pytest

- [x] **2. Segment Mapping Handlers**  
  Implement in `src/services/segment_handlers.py`:  
  ```python
  def handle_QTY(segment: str) -> dict:
      # Extract quantity data
  ```
  Repeat for LOC, DTM, MEA segments

- [x] **3. EDI → JSON Conversion Pipeline**  
  Build `src/services/edi_converter.py`:  
  ```python
  def convert_to_json(edi_data: dict) -> dict:
      # Transform EDI structure to JSON
  ```

- [x] **4. APERAK Response Simulation**  
  Add to `src/main.py`:  
  ```python
  @app.get("/edi/ack/{id}")
  async def generate_aperak(id: str):
      # Generate EDIFACT acknowledgment
  ```

- [x] **5. Verification Protocol**  
  Confirm functionality by:  
  1. Uploading sample UTILMD file  
  2. Validating JSON output matches schema  
  3. Testing APERAK response generation  
  4. Checking error handling for invalid EDI

## Phase 5: Integration & Validation
- [x] **1. Internal Message Bus Setup**  
  Extend `docker-compose.yml` with RabbitMQ:  
  ```yaml
  services:
    rabbitmq:
      image: rabbitmq:3-management
      environment:
        RABBITMQ_DEFAULT_USER: comako
        RABBITMQ_DEFAULT_PASS: comako
      ports:
        - "5672:5672"
        - "15672:15672"
      volumes:
        - rabbitmq_data:/var/lib/rabbitmq
      healthcheck:
        test: ["CMD", "rabbitmq-diagnostics", "ping"]
        interval: 30s
        timeout: 10s
        retries: 5
  ```  
  Implement async connection in `src/config.py`:  
  ```python
  import aio_pika
  
  async def get_rabbit_connection():
      return await aio_pika.connect_robust("amqp://comako:comako@localhost:5672/")
  
  async def setup_message_queues():
      # Create settlement_queue, edi_processing_queue, aperak_queue
      # Setup exchange routing with topic patterns
  ```

- [x] **2. Meter Reading → Settlement Flow**  
  Create integration in `src/services/meter_reading.py`:  
  ```python
  async def publish_reading(reading: EnergyReading):
      message_payload = {
          "reading_id": reading.id,
          "metering_point_id": reading.metering_point_id,
          "value_kwh": reading.value_kwh,
          "event_type": "meter_reading_created"
      }
      await publish_message("meter.reading.created", message_payload)
  
  class SettlementMessageConsumer:
      async def process_meter_reading_message(self, message_body):
          # Calculate deviation from forecast
          # Trigger settlement calculation
          # Log results for reporting
  ```  
  Implement message consumer with settlement calculation pipeline

- [x] **3. EDI Processing Flow**  
  Implement in `src/services/edi_processor.py`:  
  ```python
  async def publish_parsed_edi(parsed_data: dict):
      # Publish to "edi_processing" queue
  ```  
  Create Market Core subscriber for EDI messages

- [x] **4. Demo Flow Validation Scripts**  
  Create `scripts/demo_meter_flow.py`:  
  ```python
  # 1. Submit test reading
  # 2. Verify settlement calculation
  # 3. Check report generation
  ```  
  Create `scripts/demo_edi_flow.py` for EDI validation

- [x] **5. End-to-End Test Suite**  
  Build `tests/e2e/test_demo_flows.py` with:  
  ```python
  @pytest.mark.asyncio
  async def test_meter_reading_flow():
      # Submit reading
      # Verify settlement
      # Check database records
  ```  
  Configure pytest to use test containers

- [x] **6. Verification Protocol**  
  Confirm integration by:  
  1. Running `docker compose up` with all services  
  2. Executing `python scripts/demo_meter_flow.py`  
  3. Validating settlement appears in report  
  4. Running `pytest tests/e2e` → 100% pass rate

## Phase 6: SAP IS-U Compatibility Layer
- [x] **1. File-based EDI Exchange Setup**  
  Extend `docker-compose.yml` with FTP server:  
  ```yaml
  services:
    ftp:
      image: stilliard/pure-ftpd
      environment:
        PUBLICHOST: "localhost"
        FTP_USER_NAME: comako
        FTP_USER_PASS: comako
        FTP_USER_HOME: /home/comako
      ports:
        - "21:21"
        - "30000-30009:30000-30009"
  ```  
  Implement FTP client in `src/services/ftp_client.py`:  
  ```python
  from pyftpdlib.authorizers import DummyAuthorizer
  from pyftpdlib.handlers import FTPHandler
  from pyftpdlib.servers import FTPServer
  
  def start_ftp_server():
      authorizer = DummyAuthorizer()
      authorizer.add_user("comako", "comako", "/edi", perm="elradfmw")
      handler = FTPHandler
      handler.authorizer = authorizer
      server = FTPServer(("0.0.0.0", 21), handler)
      server.serve_forever()
  ```

- [x] **2. AS4 Integration Module (Primary Standard)**  
  Create `src/services/as4.py` implementing AS4 (Applicability Statement 4):  
  ```python
  from ebms3 import AS4Client, AS4Server
  
  def setup_as4_server():
      server = AS4Server(
          certificate="cert.pem",
          private_key="key.pem",
          verify=True,
          soap_version="1.2"
      )
      server.start()
  
  def send_as4_message(payload: bytes, partner: str):
      client = AS4Client()
      return client.send(
          data=payload,
          url=f"https://{partner}/as4",
          subject="EDI Message",
          from_name="CoMaKo",
          to_name=partner,
          message_properties={
              "originalSender": "COMAKO",
              "finalRecipient": partner
          }
      )
  ```
  
  **AS4 Features:**
  - Higher security, reliability and traceability than older methods
  - Web services-based B2B data exchange
  - Mandatory standard for certified software solutions
  - SOAP 1.2 with WS-Security
  - Message-level security and reliability

- [x] **2b. S/MIME Email Integration (Fallback)**  
  Create `src/services/email_edi.py` for encrypted/signed email exchange:  
  ```python
  import smtplib
  from email.mime.multipart import MIMEMultipart
  from email.mime.text import MIMEText
  from M2Crypto import SMIME, X509
  
  def send_smime_edi(edi_content: str, recipient: str):
      # Create S/MIME signed and encrypted message
      s = SMIME.SMIME()
      s.load_key('private_key.pem', 'cert.pem')
      
      msg = MIMEText(edi_content)
      signed_msg = s.sign(msg, SMIME.PKCS7_DETACHED)
      
      # Encrypt for recipient
      recipient_cert = X509.load_cert('recipient_cert.pem')
      s.set_x509_stack(X509.X509_Stack())
      s.get_x509_stack().push(recipient_cert)
      encrypted_msg = s.encrypt(signed_msg)
      
      return encrypted_msg
  ```
  
  **S/MIME Features:**
  - Original method via encrypted and signed emails
  - Still available as alternative or fallback
  - Digital signatures and encryption
  - Standard email infrastructure

- [x] **3. EDI@Energy Specification Validation**  
  Build `src/services/edi_validator.py`:  
  ```python
  def validate_edi_message(message: dict) -> bool:
      # Check required segments per EDI@Energy spec
      required_segments = ["UNB", "UNH", "BGM", "DTM"]
      return all(seg in message for seg in required_segments)
  ```  
  Integrate with EDI Gateway processing pipeline

- [x] **4. Verification Protocol**  
  Confirm compatibility by:  
  1. Sending test EDI file via FTP to SAP IS-U test system  
  2. Validating AS4 message delivery with SOAP monitoring  
  3. Testing S/MIME email fallback mechanism  
  4. Running EDI@Energy compliance checks on sample messages  
  5. Verifying successful parsing in Market Core  
  6. Confirming AS4 reliability features (receipts, retries)

## Phase 7: Documentation & Release
- [x] **1. README Enhancement**  
  Update `README.md` with:  
  - Docker setup instructions (`docker compose up`)  
  - Environment variable configuration  
  - Database migration steps  
  - Service startup commands  
  - Verification protocol examples  

- [x] **2. OpenAPI Documentation Generation**  
  Configure FastAPI to generate:  
  ```python
  app = FastAPI(
      title="CoMaKo API",
      description="Energy cooperative management system",
      version="1.0.0",
      openapi_url="/api/openapi.json"
  )
  ```  
  Create static documentation in `docs/api/` using `spectree`  

- [x] **3. Deployment Checklist Creation**  
  Build `DEPLOYMENT_CHECKLIST.md` with:  
  - [ ] Infrastructure requirements (4GB RAM, 2vCPU)  
  - [ ] Environment variable validation  
  - [ ] Database initialization verification  
  - [ ] Service health check procedures  

- [x] **4. Security Review Protocol**  
  Execute:  
  ```bash
  bandit -r src/  # Static code analysis
  safety check    # Dependency scanning
  trufflehog .    # Secret scanning
  ```  
  Document findings and remediation steps  

- [ ] **5. Verification Protocol**  
  Confirm release readiness by:  
  1. Validating all documentation renders correctly  
  2. Running security scans with zero critical issues  
  3. Completing deployment checklist on staging environment  
  4. Passing all integration tests
