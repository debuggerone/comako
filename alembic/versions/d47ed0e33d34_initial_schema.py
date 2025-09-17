"""initial schema

Revision ID: d47ed0e33d34
Revises: 
Create Date: 2025-08-03 01:55:11.815462

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd47ed0e33d34'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create ENUM types
    op.execute("CREATE TYPE meteringpointtypeenum AS ENUM ('RLM', 'SLP')")
    op.execute("CREATE TYPE energyflowdirectionenum AS ENUM ('IN', 'OUT')")
    op.execute("CREATE TYPE marketroleenum AS ENUM ('SUPPLIER', 'CUSTOMER', 'DSO', 'MSB')")

    # Create tables
    op.create_table(
        'market_participant',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('name', sa.String(), nullable=True),
        sa.Column('contact_email', sa.String(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

    op.create_table(
        'participant_roles',
        sa.Column('participant_id', sa.String(), nullable=False),
        sa.Column('role', sa.Enum('SUPPLIER', 'CUSTOMER', 'DSO', 'MSB', name='marketroleenum'), nullable=False),
        sa.Column('active_from', sa.DateTime(), nullable=True),
        sa.Column('active_to', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['participant_id'], ['market_participant.id'], ),
        sa.PrimaryKeyConstraint('participant_id', 'role')
    )

    op.create_table(
        'metering_point',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('eic_code', sa.String(), nullable=True),
        sa.Column('type', sa.Enum('RLM', 'SLP', name='meteringpointtypeenum'), nullable=True),
        sa.Column('installed_power', sa.Float(), nullable=True),
        sa.Column('injection_allowed', sa.Boolean(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

    op.create_table(
        'supply_contracts',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('metering_point_id', sa.String(), nullable=True),
        sa.Column('supplier_id', sa.String(), nullable=True),
        sa.Column('price_ct_per_kwh', sa.Integer(), nullable=True),
        sa.Column('valid_from', sa.DateTime(), nullable=True),
        sa.Column('valid_to', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['metering_point_id'], ['metering_point.id'], ),
        sa.ForeignKeyConstraint(['supplier_id'], ['market_participant.id'], ),
        sa.PrimaryKeyConstraint('id')
    )

    op.create_table(
        'balance_group',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('name', sa.String(), nullable=True),
        sa.Column('bkv_id', sa.String(), nullable=True),
        sa.ForeignKeyConstraint(['bkv_id'], ['balance_group.id'], ),
        sa.PrimaryKeyConstraint('id')
    )

    op.create_table(
        'balance_group_members',
        sa.Column('metering_point_id', sa.String(), nullable=False),
        sa.Column('balance_group_id', sa.String(), nullable=False),
        sa.Column('from_date', sa.DateTime(), nullable=True),
        sa.Column('to_date', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['metering_point_id'], ['metering_point.id'], ),
        sa.ForeignKeyConstraint(['balance_group_id'], ['balance_group.id'], ),
        sa.PrimaryKeyConstraint('metering_point_id', 'balance_group_id')
    )

    op.create_table(
        'energy_flow',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('metering_point_id', sa.String(), nullable=True),
        sa.Column('direction', sa.Enum('IN', 'OUT', name='energyflowdirectionenum'), nullable=True),
        sa.Column('source', sa.String(), nullable=True),
        sa.ForeignKeyConstraint(['metering_point_id'], ['metering_point.id'], ),
        sa.PrimaryKeyConstraint('id')
    )

    op.create_table(
        'energy_reading',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('timestamp', sa.DateTime(), nullable=True),
        sa.Column('value_kwh', sa.Float(), nullable=True),
        sa.Column('created_by', sa.String(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

    op.create_table(
        'settlement_run',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('balance_group_id', sa.String(), nullable=True),
        sa.Column('period_start', sa.DateTime(), nullable=True),
        sa.Column('period_end', sa.DateTime(), nullable=True),
        sa.Column('total_in_kwh', sa.Float(), nullable=True),
        sa.Column('total_out_kwh', sa.Float(), nullable=True),
        sa.Column('delta_kwh', sa.Float(), nullable=True),
        sa.Column('delta_cost_eur', sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(['balance_group_id'], ['balance_group.id'], ),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade() -> None:
    """Downgrade schema."""
    # Drop tables in reverse order
    op.drop_table('settlement_run')
    op.drop_table('energy_reading')
    op.drop_table('energy_flow')
    op.drop_table('balance_group_members')
    op.drop_table('balance_group')
    op.drop_table('supply_contracts')
    op.drop_table('metering_point')
    op.drop_table('participant_roles')
    op.drop_table('market_participant')

    # Drop ENUM types
    op.execute('DROP TYPE IF EXISTS meteringpointtypeenum')
    op.execute('DROP TYPE IF EXISTS energyflowdirectionenum')
    op.execute('DROP TYPE IF EXISTS marketroleenum')
