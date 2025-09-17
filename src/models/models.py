from sqlalchemy import Column, String, Integer, Float, Boolean, DateTime, Enum, ForeignKey
from sqlalchemy.orm import relationship, declarative_base
from datetime import datetime
from enum import Enum as PyEnum

Base = declarative_base()

class MarketRoleEnum(PyEnum):
    SUPPLIER = "SUPPLIER"
    CUSTOMER = "CUSTOMER"
    DSO = "DSO"
    MSB = "MSB"

class MarketParticipant(Base):
    __tablename__ = "market_participant"
    id = Column(String, primary_key=True)
    name = Column(String)
    address = Column(String)
    contact_email = Column(String)
    roles = relationship("ParticipantRoles", back_populates="participant")
    balance_group_members = relationship("BalanceGroupMember", back_populates="market_participant")
    metering_points = relationship("MeteringPoint", back_populates="market_participant")

class ParticipantRoles(Base):
    __tablename__ = "participant_roles"
    participant_id = Column(String, ForeignKey("market_participant.id"), primary_key=True)
    role = Column(Enum(MarketRoleEnum), primary_key=True)
    active_from = Column(DateTime)
    active_to = Column(DateTime)
    participant = relationship("MarketParticipant", back_populates="roles")

class MeteringPoint(Base):
    __tablename__ = "metering_point"
    id = Column(String, primary_key=True)
    eic_code = Column(String)
    type = Column(Enum("RLM", "SLP", name="meteringpointtypeenum"))
    installed_power = Column(Float)
    injection_allowed = Column(Boolean)
    market_participant_id = Column(String, ForeignKey("market_participant.id"))
    location = Column(String)
    market_participant = relationship("MarketParticipant", back_populates="metering_points")

class SupplyContracts(Base):
    __tablename__ = "supply_contracts"
    id = Column(String, primary_key=True)
    metering_point_id = Column(String, ForeignKey("metering_point.id"))
    supplier_id = Column(String, ForeignKey("market_participant.id"))
    price_ct_per_kwh = Column(Integer)
    valid_from = Column(DateTime)
    valid_to = Column(DateTime)

class BalanceGroup(Base):
    __tablename__ = "balance_group"
    id = Column(String, primary_key=True)
    name = Column(String)
    bkv_id = Column(String, ForeignKey("balance_group.id"))
    members = relationship("BalanceGroupMember", back_populates="balance_group")

class BalanceGroupMember(Base):
    __tablename__ = "balance_group_members"
    balance_group_id = Column(String, ForeignKey("balance_group.id"), primary_key=True)
    market_participant_id = Column(String, ForeignKey("market_participant.id"), primary_key=True)
    from_date = Column(DateTime)
    to_date = Column(DateTime)
    balance_group = relationship("BalanceGroup", back_populates="members")
    market_participant = relationship("MarketParticipant", back_populates="balance_group_members")

# Update MarketParticipant to include the reverse relationship

class EnergyFlow(Base):
    __tablename__ = "energy_flow"
    id = Column(String, primary_key=True)
    metering_point_id = Column(String, ForeignKey("metering_point.id"))
    direction = Column(Enum("IN", "OUT", name="energyflowdirectionenum"))
    source = Column(String)

class EnergyReading(Base):
    __tablename__ = "energy_reading"
    id = Column(String, primary_key=True)
    metering_point_id = Column(String, ForeignKey("metering_point.id"))
    timestamp = Column(DateTime)
    value_kwh = Column(Float)
    reading_type = Column(String, default="consumption")  # consumption, generation, net
    created_at = Column(DateTime, default=datetime.utcnow)
    created_by = Column(String)
    
    # Add relationship to metering point
    metering_point = relationship("MeteringPoint")

class SettlementRun(Base):
    __tablename__ = "settlement_run"
    id = Column(String, primary_key=True)
    balance_group_id = Column(String, ForeignKey("balance_group.id"))
    period_start = Column(DateTime)
    period_end = Column(DateTime)
    total_in_kwh = Column(Float)
    total_out_kwh = Column(Float)
    delta_kwh = Column(Float)
    delta_cost_eur = Column(Float)
