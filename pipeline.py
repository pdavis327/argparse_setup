"""
Pipeline operational logic.  All the real work gets done here
"""

from util.bigquery import BigqueryHelper
import rtyaml
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

class Pipeline:
    def __init__(self, args):
        self.log = logging.getLogger()
        self.dry_run = args.test
        self.bigquery = BigqueryHelper(project=args.project, logger=self.log, dry_run=self.dry_run)
        self.args = args

    @property
    def is_test_mode(self):
        return self.args.test

    @property
    def params(self):
        """
        Gets a dict version of self.args
        :return: dict
        """
        return vars(self.args)

    def fishing_hours_table_description(self):
        items = {
            "pipeline": f"{self.args.PIPELINE_NAME} v{self.args.PIPELINE_VERSION}",
            "pipeline-description": self.args.PIPELINE_DESCRIPTION,
            "table-description": self.args.table_description,
            "arguments": self.params
        }
        return rtyaml.dump(items)
    
    def agg_segs_query(self):
        return self.bigquery.format_query('assets/agg_segs.sql.j2', **self.params)

    def validation_query(self):
        return self.bigquery.format_query('assets/validate.sql.j2', **self.params)

    def run_daily_sat_timing(self):
        start_date = self.args.start_date
        end_date = self.args.end_date
        ais_positions_table = self.args.messages_scored_table
        norad_lookup = self.args.norad_receiver_lookup
        sat_positions = self.args.satellite_positions_table
        dest_table = self.args.dest_satellite_timing_table
        success = True

        d = datetime.strptime(start_date, "%Y-%m-%d")
        endtime = datetime.strptime(end_date, "%Y-%m-%d")
        tp = []
        while d <= endtime:
            tp.append(d.strftime("%Y%m%d"))
            d = d + timedelta(days=1)
        
        commands = {}
        for t in tp:
            destination_table = dest_table + "$" + t
            YYYY_MM_DD = t[:4] + "-" + t[4:6] + "-" + t[6:8]
            command = self.bigquery.format_query(
                'assets/daily_sat_timing.sql.j2',
                date = YYYY_MM_DD,
                positions_table = ais_positions_table,
                norad_receiver_lookup = norad_lookup,
                sat_positions  = sat_positions)
            commands[destination_table] = command

        # check for table
        self.bigquery.check_create_table(dest_table)

        print('Running satellite timing query')
        with ThreadPoolExecutor() as e:
            for c in commands:
                e.submit(self.bigquery.run_query, 
                        query = commands[c], 
                        dest_table = c)
        
        self.log.info(f'run_daily_sat_timing completed successfully')

        return success

    def run_segs_daily(self):
        start_date = self.args.start_date
        end_date = self.args.end_date
        dest_table = self.args.dest_segs_daily_table
        sat_timing_out = self.args.dest_satellite_timing_table
        rad_messages = self.args.research_messages_table
        success = True

        d = datetime.strptime(start_date, "%Y-%m-%d")
        endtime = datetime.strptime(end_date, "%Y-%m-%d")
        tp = []
        while d <= endtime:
            tp.append(d.strftime("%Y%m%d"))
            d = d + timedelta(days=1)

        commands = {}
        for t in tp:
            destination_table = dest_table + "$" + t
            YYYY_MM_DD = t[:4] + "-" + t[4:6] + "-" + t[6:8]
            command = self.bigquery.format_query(
                'assets/segs_daily.sql.j2',
                date = YYYY_MM_DD,
                sat_timing_table = sat_timing_out,
                research_messages  = rad_messages, 
                )
            commands[destination_table] = command

        # check for table
        self.bigquery.check_create_table(segs_daily_out)


        with ThreadPoolExecutor() as e:
            for c in commands:
                e.submit(self.bigquery.run_query, 
                        query = commands[c], 
                        dest_table = c)
        
        self.log.info(f'Completed successfully')

        return success

#good above?


    def run_fishing_hours(self):
        query = self.fishing_hours_query()
        partition_field = 'start_date'
        schema = 'assets/fishing_hours.schema.json'
        table_desc = self.fishing_hours_table_description()
        dest_table = self.args.dest_fishing_hours_flag_table
        start_date = self.args.start_date
        success = True

        if self.is_test_mode:
            self.log.info(query)
        else:
            # create table
            self.log.info(f'Creating table {dest_table} if not exists')
            self.bigquery.create_table(dest_table,
                                       schema,
                                       table_desc,
                                       partition_field=partition_field)

            # clear out the target partition so we don't get duplicates
            self.log.info(f'Clearing out partition where {partition_field} = {start_date}')
            self.bigquery.clear_table_partition(dest_table,
                                                partition_field,
                                                start_date)
            # run the query
            self.log.info(f'Writing fishing hours by flag to table {dest_table}')
            self.bigquery.run_query(query, dest_table)

            self.log.info(f'run_segs_daily completed successfully')

        return success

    def run_validation(self):
        query = self.validation_query()

        result = self.bigquery.run_query(query)
        success = all(row['success'] for row in result)

        if success:
            print ("Validation succeeded.")
        else:
            print ("Validation failed.")

            if success:
                print("Validation succeeded.")
            else:
                print("Validation failed.")
        return success

    def run(self):
        if self.args.operation == 'fishing_hours':
            return self.run_fishing_hours()
        elif self.args.operation == 'validate':
            return self.run_validation()
        else:
            raise RuntimeError(f'Invalid operation: {self.args.operation}')

        return False    # should not be able to get here, but just in case, return failure
