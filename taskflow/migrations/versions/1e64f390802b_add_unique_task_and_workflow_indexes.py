"""add unique task and workflow indexes

Revision ID: 1e64f390802b
Revises: 6ef79b56ad4a
Create Date: 2017-06-19 17:02:25.402708+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1e64f390802b'
down_revision = '6ef79b56ad4a'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE UNIQUE INDEX index_unique_workflow ON workflow_instances USING btree (workflow_name, \"unique\") WHERE status = ANY (ARRAY['queued'::taskflow_statuses, 'pushed'::taskflow_statuses, 'running'::taskflow_statuses, 'retry'::taskflow_statuses]);")
    op.execute("CREATE UNIQUE INDEX index_unique_task ON task_instances USING btree (task_name, \"unique\") WHERE status = ANY (ARRAY['queued'::taskflow_statuses, 'pushed'::taskflow_statuses, 'running'::taskflow_statuses, 'retry'::taskflow_statuses]);")

def downgrade():
    op.execute('DROP INDEX index_unique_workflow;')
    op.execute('DROP INDEX index_unique_task;')
