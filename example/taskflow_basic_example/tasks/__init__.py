from .vacuum_postgres import vacuum_events, vacuum_aggregates

tasks = [
    vacuum_events,
    vacuum_aggregates
]
