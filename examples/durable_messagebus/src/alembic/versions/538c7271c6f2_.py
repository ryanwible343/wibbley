"""empty message

Revision ID: 538c7271c6f2
Revises: 
Create Date: 2024-02-13 04:55:15.014611

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '538c7271c6f2'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('shape',
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('type', sa.String(), nullable=True),
    sa.Column('volume', sa.Integer(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('shape')
    # ### end Alembic commands ###
