# ⚡ CoMaKo – Community Market Communication Platform

## 🧓🏴‍☠️ For Retirees with Responsibility & Energy Rebels with Vision

**CoMaKo** is an open-source backend platform for people who are tired of selling their solar power for pennies.  
It’s made for:
- **Photovoltaic (PV) owners** who want to share their electricity inside a cooperative — at **10 ct/kWh**, not the ~6 ct feed-in tariff.
- **Retirees or neighbors** who volunteer to run the system and keep the energy flowing.
- **Energy rebels** who don’t wait for policy to catch up — they build their own solution.

> Whether you report your meter reading via voicebot, CSV upload, or web form — the energy revolution starts in the backend.

---

## 📝 Project Summary

**CoMaKo** is a modular, API-driven backend system for local energy cooperatives that want to operate as real utilities.  
It combines:
- Germany’s regulated energy market communication (MaKo 2020 / EDI@Energy)
- Modern REST APIs for flexible user interaction
- Transparent internal billing and balance group logic

---

## 🌞 Real-World Use Case: PV Sharing in Cooperatives

### 💥 The Problem:
Feed-in tariffs for small solar installations are low (~6–7 ct/kWh).  
Selling power to the grid means losing most of the value your panels produce.

### ✅ The CoMaKo Solution:
- PV owners **feed their electricity into a cooperative balance group**
- Other members consume it at a **fixed fair price** (e.g. 10 ct/kWh)
- The system **tracks production, consumption, and cost** internally
- Optionally, it interfaces with the national grid and external market participants via EDI

> It’s energy democracy — for your street, your village, your housing block.

---

## 🔧 System Components

### 🧩 `edi_gateway`
- Accepts `.edi` files (UTILMD, MSCONS, INVOIC, etc.)
- Parses and validates EDI@Energy messages
- Converts EDI → JSON and simulates APERAK responses

### 🧩 `market_core` (MCP)
- Stores metering points, participants, and balance groups
- Runs the **Balance Group Agent**
- Calculates internal consumption, generation, and settlement
- Generates cost and energy flow reports

### 🧩 `meter_gateway`
- Provides REST API for submitting meter readings
- Accepts data from voicebots, apps, or CSVs
- Validates and forwards data to the core system

---

## 🤖 Automation Agents

### 🧠 Balance Group Agent
- Aggregates usage across all participants
- Compares actual consumption vs. forecast
- Calculates deviation and balancing cost
- Settles internal pricing at **10 ct/kWh**

~~~json
{
  "balance_group": "BK123456",
  "period": "2025-08-01 to 2025-08-02",
  "total_consumed_kwh": 1387.2,
  "total_generated_kwh": 1420.0,
  "internal_price_ct_per_kwh": 10,
  "settlement": {
    "ZP001": { "usage_kwh": 510.2, "cost_eur": 51.02 },
    "ZP002": { "usage_kwh": 877.0, "cost_eur": 87.70 }
  }
}
~~~

### 🧠 EDI Agent
- Parses EDIFACT-formatted energy messages
- Maps segments like `QTY`, `LOC`, `DTM`, `MEA`, etc.
- Generates clean, developer-friendly JSON

~~~plaintext
UNA:+.? '
UNB+UNOC:3+DE0001234567+DE9876543210+230802:1200+MSG0001'
UNH+1+UTILMD:D:01B:UN:1.1'
BGM+Z01+123456789+9'
NAD+MS+DE0001234567::9'
LOC+172+DE0123456789012345670000000000001'
...
UNT+23+1'
UNZ+1+MSG0001'
~~~

---

## 🔌 API Overview

### `/meter_gateway`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/readings` | Submit meter reading |
| `GET`  | `/readings/{id}` | Get reading history |
| `GET`  | `/readings/anomalies` | Detect outliers |

### `/market_core`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET`  | `/balance_groups/{id}` | View balance group info |
| `GET`  | `/balance_groups/{id}/report` | Generate report |
| `POST` | `/consumption` | Import consumption data |

### `/edi_gateway`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/edi/upload` | Upload .edi file |
| `GET`  | `/edi/messages` | View parsed messages |
| `GET`  | `/edi/ack/{id}` | Simulate APERAK response |

---

## 📂 Sample Data Model

~~~json
{
  "balance_groups": [
    {
      "id": "BK123456",
      "name": "GreenCoop Energy",
      "metering_points": ["ZP001", "ZP002"]
    }
  ],
  "readings": [
    {
      "metering_point": "ZP001",
      "timestamp": "2025-08-02T12:00:00Z",
      "consumption_kwh": 5.25
    }
  ]
}
~~~

---

## 🛠 Tech Stack

| Component        | Technology         |
|------------------|--------------------|
| API Backend       | Python (FastAPI)   |
| Database          | PostgreSQL         |
| EDI Parsing       | bots-edi or custom |
| Containerization  | Docker Compose     |
| Agent Logic       | Python modules     |

---

## 🚀 Getting Started

Follow these steps to get the CoMaKo platform running on your local machine.

### Prerequisites
- **Docker** & **Docker Compose**
- **Python 3.10+**
- **Git**

### 1. Clone the Repository
```bash
git clone <repository-url>
cd comako
```

### 2. Set Up Environment
The application uses environment variables for configuration. While defaults are provided, you can create a `.env` file in the root directory to override them. Key variables include:
- `DATABASE_URL`
- `RABBITMQ_URL`
- `FTP_HOST`, `FTP_USER`, `FTP_PASS`
- `AS2_CERTIFICATE_PATH`, `AS2_PRIVATE_KEY_PATH`
- `AS4_CERTIFICATE_PATH`, `AS4_PRIVATE_KEY_PATH`

### 3. Start Services
The `docker-compose.yml` file includes all necessary services (PostgreSQL, RabbitMQ, FTP server). Start them in detached mode:
```bash
docker compose up -d
```
This will start the database, message bus, and FTP server required for full functionality.

### 4. Install Dependencies & Run Migrations
It's recommended to use a virtual environment.
```bash
python -m venv venv
source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
pip install -r requirements.txt
```
Once dependencies are installed, apply the database schema:
```bash
alembic upgrade head
```

### 5. Start the Application
Run the FastAPI server:
```bash
uvicorn src.main:app --reload --host 0.0.0.0 --port 8000
```
The API will be available at `http://localhost:8000`.

---

## 🔬 Verification & Usage

After setup, you can verify that all components are working correctly.

### 1. Health Check
Check the API and database connection:
```bash
curl http://localhost:8000/health
```
Expected response: `{"status":"ok","db":"connected"}`

### 2. Run Demo Scripts
The `scripts/` directory contains scripts to demonstrate end-to-end flows.
```bash
# Example: Run the complete meter reading and settlement flow
python scripts/demo_complete_flow.py

# Example: Run the EDI processing flow
python scripts/demo_edi_flow.py
```

### 3. Run the Test Suite
Execute the full test suite to ensure all modules are functioning as expected:
```bash
pytest -v
```

---

## 🌐 Compatible with SAP IS-U

CoMaKo can interface with real SAP IS-U systems using:
- **File-based EDI Exchange**: The built-in FTP server (`/edi` directory) can be used for exchanging UTILMD, MSCONS, and other EDI files.
- **AS4 (Primary)**: The modern, secure standard for B2B communication. CoMaKo implements a fully-featured AS4 server and client.
- **AS2 (Fallback)**: A widely supported legacy protocol for secure messaging.
- **EDI@Energy Validation**: All incoming and outgoing messages are validated against the official EDI@Energy specifications.

---

## 👐 Open-Source & Community-Owned

**CoMaKo is 100% open-source and free to use.**

- Suggested internal electricity price: **10 ct/kWh**
- No subscription. No license cost. No vendor lock-in.
- Fork it, host it, or use it as a template for your own neighborhood energy coop.

> Made for housing collectives, eco-villages, solar rooftop rebels, and anyone who believes electricity should be community-owned.

---

## ⚡ License

[MIT License](LICENCE) – feel free to use, remix, and redistribute.

---
