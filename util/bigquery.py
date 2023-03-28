"""
Utilities for use with bigquery
"""
import datetime as dt
import logging
from time import sleep
from util.datetime import as_date_str
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from jinja2 import Environment
from jinja2 import FileSystemLoader
from jinja2 import StrictUndefined
import json


def format_query(template_file, **params):
    """
    Format a jinja2 templated query with the given params.

    You may have extra params which are not used by the template, but all params
    required by the template must be provided.
    """

    search_paths = ['./', './assets/']
    jinja2_env = Environment(loader=FileSystemLoader(search_paths), undefined=StrictUndefined)
    sql_template = jinja2_env.get_template(template_file)

    formatted_template = sql_template.render(params)
    return formatted_template


def load_schema(schema_file):
    with open(schema_file) as file:
        return json.load(file)


class BigqueryHelper:
    def __init__(self, project=None, logger=None, dry_run=False):
        self.project = project
        self.log = logger or logging.getLogger(__name__)
        self.dry_run = dry_run
        self.client = bigquery.Client(project=project)

    def format_query(self, template_file, **params):
        return format_query(template_file, **params)

    def begin_session(self):
        config = bigquery.QueryJobConfig(
            create_session=True,
            dry_run=self.dry_run,
        )
        self.log.info('Starting bigquery session')
        job = self.client.query("SELECT 1", job_config=config)
        session_id = job.session_info.session_id
        self.log.info(f'Bigquery session started {session_id}')
        return session_id

    def end_session(self, session_id):
        config = bigquery.QueryJobConfig(
            dry_run=self.dry_run,
        )
        self.log.info(f'Terminating bigquery session {session_id}')
        job = self.client.query(f"CALL BQ.ABORT_SESSION('{session_id}')", job_config=config)
        result = job.result()

    def create_table(self, 
                     full_table_name, 
                     schema_file=None, 
                     table_description=None, 
                     partition_field=None, 
                     exists_ok=True,
                     time_partitioning_day= False):
        """
        Create a BigQuery table.

        :param full_table_name: fully qualified table name 'project_id.dataset.table'
        :param schema_file: path to schema json file
        :param table_description: text to include in the table's description field
        :param partition_field: name of field to use for time partitioning (None for no partition)
        :param exists_ok: Defaults to True. If True, ignore “already exists” errors when creating the table.
        :return: A new google.cloud.bigquery.table.Table
        """

        # load schema
        if schema_file:
            schema = load_schema(schema_file)
            table_ref = self.table_ref(full_table_name)

        # Create BQ table object
        bq_table = bigquery.Table(table_ref, schema=schema)

        # Set table description
        if table_description:
            bq_table.description = table_description

        # Set partitioning
        if partition_field:
            bq_table.time_partitioning = bigquery.TimePartitioning(field=partition_field)
        
        if time_partitioning_day:
            bq_table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY
        )

        # Create table
        return self.client.create_table(bq_table, exists_ok=exists_ok)

    def check_create_table(
            self, 
            table_name, 
            table_description=None,
            partition_field=None,
            time_partitioning_day= False,
            exists_ok=True):
        '''
        Checks if the table exists, and if not creates it. 
        Args:
            full_table_name: full_table_name: fully qualified table name
                'project_id.dataset.table'
            schema_file: path to schema json file
            table_description: text to include in the table's description field
            partition_field: name of field to use for time partitioning.
                Defaults to None.
            exists_ok: ignore “already exists” errors when creating the table.
                Defaults to True.
        Returns:
            A new google.cloud.bigquery.table.Table
        '''
        try:
            self.client.get_table(table_name)
        except NotFound:
            # create table
            print(f"Creating table {table_name}")
            return self.create_table(table_name, 
                              table_description=table_description,
                                partition_field=partition_field,
                                time_partitioning_day= True,
                                exists_ok=exists_ok)

    def clear_table_partition(self, full_table_name, partition_field, partition_date):
        """
        Clear data from a specific table partition

        partition_date may be a datetime, date or a string formatted 'YYYY-MM-DD'
        """

        partition_date = as_date_str(partition_date)

        # Clear data for query date before running query
        d_q = f"DELETE FROM `{full_table_name}` WHERE DATE({partition_field}) = '{partition_date}'"

        config = bigquery.QueryJobConfig(priority="BATCH")
        job = self.client.query(d_q, job_config=config)

        if job.error_result:
            err = job.error_result["reason"]
            msg = job.error_result["message"]
            raise RuntimeError(f'{err}: {msg}')
        else:
            job.result()

    def table_ref(self, table):
        return bigquery.TableReference.from_string(table, default_project=self.client.project)

    def update_table_description(self, table, description):
        table = self.client.get_table(self.table_ref(table))  # API request
        table.description = description
        table = self.client.update_table(table, ["description"])  # API request

    def timeline_stats(self, timeline):
        stats = {
            'elapsed_ms': 0,
            'slot_millis': 0,
            'pending_units': '',
            'completed_units': '',
            'active_units': ''
        }
        if timeline:
            entry = timeline[-1]
            stats['elapsed_ms'] = int(entry.elapsed_ms)
            stats['slot_millis'] = int(entry.slot_millis)
            stats['pending_units'] = entry.pending_units
            stats['completed_units'] = entry.completed_units
            stats['active_units'] = entry.active_units
        return stats

    def render_query_plan_entry(self, entry):
        entry = {'name': 'S00: Input', 'id': '0', 'startMs': '1655493911998', 'endMs': '1655493912149', 'waitRatioAvg': 0, 'waitMsAvg': '0', 'waitRatioMax': 0, 'waitMsMax': '0', 'readRatioAvg': 5.5089047510368544e-05, 'readMsAvg': '7', 'readRatioMax': 5.5089047510368544e-05, 'readMsMax': '7', 'computeRatioAvg': 0.0017235002006815302, 'computeMsAvg': '219', 'computeRatioMax': 0.0017235002006815302, 'computeMsMax': '219', 'writeRatioAvg': 0.0001259178228808424, 'writeMsAvg': '16', 'writeRatioMax': 0.0001259178228808424, 'writeMsMax': '16', 'shuffleOutputBytes': '13941359', 'shuffleOutputBytesSpilled': '0', 'recordsRead': '61200', 'recordsWritten': '61200', 'parallelInputs': '1', 'completedParallelInputs': '1', 'status': 'COMPLETE', 'steps': [{'kind': 'READ', 'substeps': ['$10:grid_id, $11:lat_min, $12:lat_max, $13:lon_min, $14:lon_max, $15:geo', 'FROM world-fishing-827.scratch_pipe_regions_dev_ttl_100.grid_100km']}, {'kind': 'WRITE', 'substeps': ['$10, $11, $12, $13, $14, $15', 'TO __stage00_output']}], 'slotMs': '248'}

    def render_query_plan(self, query_plan):
        return [self.render_query_plan_entry(entry) for entry in query_plan]

    def log_job_stats(self, job):
        execution_seconds = (job.ended - job.started).total_seconds()
        slot_seconds = (job.slot_millis or 0) / 1000
        self.log.debug(f'  execution_seconds:     {execution_seconds}')
        self.log.debug(f'  slot_seconds:          {slot_seconds}')
        self.log.debug(f'  num_child_jobs:        {job.num_child_jobs}')
        self.log.debug(f'  total_bytes_processed: {job.total_bytes_processed}')
        self.log.debug(f'  total_bytes_billed: {job.total_bytes_billed}')
        self.log.debug(f'  referenced_tables:')
        for table in job.referenced_tables:
            self.log.debug(f'    {table.project}.{table.dataset_id}.{table.table_id}')
        if job.destination:
            self.log.debug('  output_table:')
            self.log.debug(f'    {job.destination}')

        self.log.debug(f'  reservation_usage:     {job.reservation_usage}')
        self.log.debug(f'  script_statistics:     {job.script_statistics}')
        # print(f'query_plan:')
        # for entry in job.query_plan:
        #     print(entry._properties)

    def dump_query(self, query):
        self.log.warning('\n*** BEGIN SQL ***\n')
        self.log.warning(query)
        self.log.warning('\n*** END SQL ***\n')

    def run_query(self, query,
                  dest_table=None,
                  write_disposition="WRITE_APPEND",
                  clustering_fields=None,
                  session_id=None):

        connection_properties = []
        if session_id is not None:
            connection_properties.append(bigquery.ConnectionProperty(key='session_id', value=session_id))

        if dest_table:
            config = bigquery.QueryJobConfig(
                destination=self.table_ref(dest_table),
                priority="BATCH",
                write_disposition=write_disposition,
                dry_run=self.dry_run,
                clustering_fields=clustering_fields,
                connection_properties=connection_properties
            )
        else:
            config = bigquery.QueryJobConfig(
                priority="INTERACTIVE",
                dry_run=self.dry_run,
                connection_properties=connection_properties
            )
        job = self.client.query(query, job_config=config)

        if self.dry_run:
            self.log.info("\n*** DRY RUN ***\n")
        self.log.info(f'Bigquery job created: {job.created:%Y-%m-%d %H:%M:%S}')
        self.log.debug(f'job_id: {job.job_id}')
        self.log.debug(f'priority: {job.priority}')
        self.log.debug('Running...')
        start_time = dt.datetime.now()
        i = 0
        while job.running():
            stats = self.timeline_stats(job.timeline)
            stats['run_time'] = round((dt.datetime.now() - start_time).total_seconds()) + 1
            stats['exec_s'] = round(stats['elapsed_ms'] / 1000)
            stats['slot_s'] = round(stats['slot_millis'] / 1000)
            if i % 10 == 0:
                line = '  elapsed: {run_time}s ' \
                       ' exec: {exec_s}s  slot: {slot_s}s' \
                       ' pend: {pending_units} compl: {completed_units} active: {active_units}'.format(**stats)
                line = f'{line[:80]: <80}'
                if self.log.level == logging.DEBUG:
                    print(line, end='\r', flush=True)
            i += 1
            sleep(0.1)
        if self.log.level == logging.DEBUG:
            print(' ' * 80)     # overwrite the previous line
        self.log.info("Bigquery job done.")

        if job.dry_run:
            self.dump_query(job.query)
        elif job.error_result:
            err = job.error_result["reason"]
            msg = job.error_result["message"]
            raise RuntimeError(f'{err}: {msg}')
        else:
            self.log_job_stats(job)

        return job.result()
