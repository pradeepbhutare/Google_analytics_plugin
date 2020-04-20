import sys,os
from bson import json_util
import json,csv
from airflow.contrib.hooks.google_analytics_hook import GoogleAnalyticsHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
import ast
from datetime import date, datetime
from airflow.utils.decorators import apply_defaults
from tempfile import NamedTemporaryFile



class Google_analytics_to_gcs(BaseOperator):
    # TODO This currently sets job = queued and locks job

    def __init__(self,
                 google_analytics_conn_id,
                 dimensions,
                 metrics,
                 viewid,
                 startDate,
                 endDate,
                 delegate_to,
                 pagesize,
                 filename,
                 bucket,
                 gcs_conn_id,
                 *args, **kwargs):
        super(Google_analytics_to_gcs, self).__init__(*args, **kwargs)
        self.google_analytics_conn_id = google_analytics_conn_id
        self.dimensions = dimensions
        self.metrics = metrics
        self.viewid = viewid
        self.startDate = startDate
        self.endDate = endDate
        self.delegate_to = delegate_to
        self.filename = filename
        self.bucket = bucket
        self.google_cloud_storage_conn_id = gcs_conn_id
        self.pagesize = pagesize
        if self.pagesize > 10000:
            raise Exception('Please specify a page size equal to or lower than 10000.')


    def execute(self, context):
        response = self.get_ga_doc()
        print(response)

        self.save_report_data(response)

        self.upload_to_gcs()


    def get_ga_doc(self):

        analytics = GoogleAnalyticsHook(self.google_analytics_conn_id).get_service_object()

        dimension = self.dimensions
        metric = self.metrics

        dim = [x for (x) in dimension.split(',') if x]
        mat = [x for (x) in metric.split(',') if x]

        dim_var = ["{'name': " + "'" + s + "'" + '}' for s in dim]
        mat_Var = ["{'expression': " + "'" + s + "'" + '}' for s in mat]

        dim_data = [ast.literal_eval(x) for x in dim_var]
        mat_data = [ast.literal_eval(x) for x in mat_Var]

        reportRequest = {
            'viewId': self.viewid,
            'dateRanges': [{'startDate': self.startDate, 'endDate': self.endDate}],
            'dimensions': dim_data,
            'metrics': mat_data,
            'pageSize': self.pagesize
        }

        response = (analytics
                    .reports()
                    .batchGet(body={'reportRequests': [reportRequest]})
                    .execute())


        if response.get('reports'):
            return response
        else:
            raise Exception('Not able to connect GA.')



    def save_report_data(self, response):
        for report in response.get('reports', []):

            f = open(self.filename, "w")
            writer = csv.writer(f, lineterminator='\n')

            columnHeader = report.get('columnHeader', {})
            dimensionHeaders = columnHeader.get('dimensions', [])
            metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
            rows = report.get('data', {}).get('rows', [])

            header_row = []
            temp_header_row = []
            header_row.extend(dimensionHeaders)
            header_row.extend([mh['name'] for mh in metricHeaders])
            temp_header_row.extend(header_row)

            writer.writerow(temp_header_row)

            for row in rows:
                dimensions_data = row.get('dimensions', [])
                metrics_data = [m['values'] for m in row.get('metrics', [])][0]

                data_row = []
                temp_data_row = []
                data_row.extend(dimensions_data)
                data_row.extend(metrics_data)

                temp_data_row.extend(data_row)
                writer.writerow(temp_data_row)

            # Close the file.
            f.close()
        # with open(self.filename, 'rt')as read:
        #     data = csv.reader(read)
        #     print("data from csv file")
        #     for row in data:
        #         print(row)


    def upload_to_gcs(self):
        """
        Upload all of the file splits (and optionally the schema .json file) to
        Google cloud storage.
        """
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        hook.upload(self.bucket,self.filename,self.filename,'text/csv')
        os.remove(self.filename)

class google_analytics_to_gcs(AirflowPlugin):
    name = 'google_analytics_to_gcs'
    operators = [Google_analytics_to_gcs]

