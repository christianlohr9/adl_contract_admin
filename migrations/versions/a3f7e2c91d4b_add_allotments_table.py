"""add allotments table

Revision ID: a3f7e2c91d4b
Revises: 08db7025a178
Create Date: 2026-03-11 18:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a3f7e2c91d4b'
down_revision: Union[str, Sequence[str], None] = '08db7025a178'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table('allotments',
        sa.Column('team_id', sa.Integer(), nullable=False, comment='Team that used the allotment'),
        sa.Column('season', sa.Integer(), nullable=False, comment='ADL season year'),
        sa.Column('action_type', sa.String(length=20), nullable=False,
                  comment='Contract tool type: franchise_tag, buyout_restructure, tender, july_1_tender'),
        sa.Column('player_id', sa.Integer(), nullable=True,
                  comment='Player the allotment was used on (nullable)'),
        sa.Column('used_at', sa.DateTime(), nullable=False, comment='When the allotment was consumed'),
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.ForeignKeyConstraint(['player_id'], ['players.id']),
        sa.ForeignKeyConstraint(['team_id'], ['teams.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('team_id', 'season', 'action_type', 'player_id',
                            name='uq_allotment_team_season_action_player'),
    )
    op.create_index(op.f('ix_allotments_team_id'), 'allotments', ['team_id'], unique=False)
    op.create_index(op.f('ix_allotments_player_id'), 'allotments', ['player_id'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f('ix_allotments_player_id'), table_name='allotments')
    op.drop_index(op.f('ix_allotments_team_id'), table_name='allotments')
    op.drop_table('allotments')
