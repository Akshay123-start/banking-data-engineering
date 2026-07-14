import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from configs.config import PROJECT_ID, DATASET_ID,CUSTOMER_TABLE_ID
class ParseCustomer(beam.DoFn):
    def process(self, line):
        if line.startswith("customer_id"):
            return
        fields = line.split(",")
        yield {
            "customer_id": fields[0],
            "customer_name": fields[1],
            "age": int(fields[2]),
            "city": fields[3],
            "phone": fields[4],
            "email": fields[5]
        }
class ValidateCustomer(beam.DoFn):
    def process(self, record):
        if record["age"] <= 0:
            return
        if "@" not in record["email"]:
            return
        yield record       
table_schema = {
    "fields": [
        {"name": "customer_id", "type": "STRING"},
        {"name": "customer_name", "type": "STRING"},
        {"name": "age", "type": "INTEGER"},
        {"name": "city", "type": "STRING"},
        {"name": "phone", "type": "STRING"},
        {"name": "email", "type": "STRING"}
    ]
}        
with beam.Pipeline() as p:
    (
        p
        | "Read Customers" >> beam.io.ReadFromText("data/customers.csv", skip_header_lines=1)
        | "Parse Customers" >> beam.ParDo(ParseCustomer())
        | "Validate Customers" >> beam.ParDo(ValidateCustomer())
        | "Write To BigQuery" >> WriteToBigQuery(table=f"{PROJECT_ID}:{DATASET_ID}.{CUSTOMER_TABLE_ID}",schema=table_schema,custom_gcs_temp_location="gs://akshay-data-engineer-2026/temp",write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)
        )