
```ascii
# Database Schema for Comako
# This schema defines the core entities and relationships for the Comako energy management system.

+--------------------+
|     MarketRole     |  ← ENUM: 'SUPPLIER', 'CUSTOMER', 'DSO', 'MSB', etc.
+--------------------+

+--------------------+       +------------------------+
|   MarketParticipant|<----->|   ParticipantRoles     |
+--------------------+       +------------------------+
| id (UUID)          |       | participant_id         |
| name               |       | role (enum ref)        |
| contact_email      |       | active_from/to         |
+--------------------+       +------------------------+

+--------------------+       +------------------------+
|   MeteringPoint    |<----->|   SupplyContracts      |
+--------------------+       +------------------------+
| id (UUID)          |       | id                     |
| EIC code           |       | metering_point_id      |
| type: 'RLM/SLP'    |       | supplier_id            |
| installed_power    |       | price_ct_per_kwh       |
| injection_allowed  |       | valid_from/to          |
+--------------------+       +------------------------+

+--------------------+       +------------------------+
|   BalanceGroup     |<----->|  BalanceGroupMembers   |
+--------------------+       +------------------------+
| id                 |       | metering_point_id      |
| name               |       | balance_group_id       |
| bkv_id (FK)        |       | from/to (optional)     |
+--------------------+       +------------------------+

+--------------------+       +------------------------+
|     EnergyFlow     |<----->|    EnergyReading       |
+--------------------+       +------------------------+
| id                 |       | id                     |
| metering_point_id  |       | timestamp              |
| direction: IN/OUT  |       | value_kwh              |
| source: MSCONS/API |       | created_by             |
+--------------------+       +------------------------+

+--------------------+
|   SettlementRun    |
+--------------------+
| id                 |
| balance_group_id   |
| period_start/end   |
| total_in_kwh       |
| total_out_kwh      |
| delta_kwh          |
| delta_cost_eur     |
+--------------------+
```
