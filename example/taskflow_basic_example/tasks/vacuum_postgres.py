import os

from taskflow.tasks.bash_task import BashTask

class VacuumPostgres(BashTask):
    def get_command(self):
        return 'psql -c "VACUUM ANALYZE {}" -h {} -p {} -u "$POSTGRES_USERNAME"'.format(
            self.params['table_name'],
            self.params['host'],
            self.params['port'])

vacuum_events = VacuumPostgres(
    name='vacuum_events',
    active=True,
    schedule='0 8 * * 2-6', # Every Tuesday through Saturday at 8am UTC
    timeout=3600,
    params={
        'table_name': 'events',
        'host': 'localhost',
        'port': 5432
    })

vacuum_aggregates = VacuumPostgres(
    name='vacuum_aggregates',
    active=True,
    schedule='0 8 * * 2-6', # Every Tuesday through Saturday at 8am UTC
    timeout=3600,
    params={
        'table_name': 'user_aggregate_metrics',
        'host': 'localhost',
        'port': 5432
    })
