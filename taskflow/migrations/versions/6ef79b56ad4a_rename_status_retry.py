"""rename status retry

Revision ID: 6ef79b56ad4a
Revises: 94e7b91d83d5
Create Date: 2017-06-19 15:06:50.441524+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6ef79b56ad4a'
down_revision = '94e7b91d83d5'
branch_labels = None
depends_on = None

## ref http://blog.yo1.dog/updating-enum-values-in-postgresql-the-safe-and-easy-way/

def upgrade():
    op.execute("""
        BEGIN;
        ALTER TYPE taskflow_statuses RENAME TO taskflow_statuses_old;
        CREATE TYPE taskflow_statuses AS ENUM('queued','pushed','running','retry','dequeued','failed','success');
        CREATE TYPE taskflow_statuses_inter AS ENUM('queued','pushed','running','retry','retrying','dequeued','failed','success');
        ALTER TABLE task_instances ALTER COLUMN status TYPE taskflow_statuses_inter USING status::text::taskflow_statuses_inter;
        UPDATE task_instances SET status = 'retry' WHERE status = 'retrying';
        ALTER TABLE task_instances ALTER COLUMN status TYPE taskflow_statuses USING status::text::taskflow_statuses;
        DROP TYPE taskflow_statuses_old;
        DROP TYPE taskflow_statuses_inter;
        COMMIT;
        """)

def downgrade():
    op.execute("""
        BEGIN;
        ALTER TYPE taskflow_statuses RENAME TO taskflow_statuses_old;
        CREATE TYPE taskflow_statuses AS ENUM('queued','pushed','running','retrying','dequeued','failed','success');
        CREATE TYPE taskflow_statuses_inter AS ENUM('queued','pushed','running','retry','retrying','dequeued','failed','success');
        ALTER TABLE task_instances ALTER COLUMN status TYPE taskflow_statuses_inter USING status::text::taskflow_statuses_inter;
        UPDATE task_instances SET status = 'retrying' WHERE status = 'retry';
        ALTER TABLE task_instances ALTER COLUMN status TYPE taskflow_statuses USING status::text::taskflow_statuses;
        DROP TYPE taskflow_statuses_old;
        DROP TYPE taskflow_statuses_inter;
        COMMIT;
        """)
