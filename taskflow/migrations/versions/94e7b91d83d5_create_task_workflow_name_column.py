"""Create Task workflow_name column

Revision ID: 94e7b91d83d5
Revises: 
Create Date: 2017-06-14 02:33:42.738473+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '94e7b91d83d5'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('tasks',
        sa.Column('workflow_name', sa.String, sa.ForeignKey('workflows.name'))
    )


def downgrade():
    op.drop_column('tasks', 'workflow_name')
