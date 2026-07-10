import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from configs.config import PROJECT_ID, DATASET_ID, TABLE_ID,BUCKET_NAME
class ParseAccount(beam.DoFn):
  def process(self, line):
    fields = line.split(",")
    yield {
      "account_id": fields[0],
      "customer_id" : fields[1],
      "account_type" : fields[2],
      "balance" : float(fields[3]),
      "status" : fields[4],
      "created_date" : fields[5],
      "branch_name": fields[6]
    }
class ValidateAccounts(beam.DoFn):
  def process(self, record):
    if record["balance"] <= 0:
      return
    if record["status"] not in ["Active", "Inactive"]:
      return
    yield record
table_schema = {
  "fields": [
    {"name": "account_id", "type": "STRING"},
    {"name": "customer_id", "type": "STRING"},
    {"name": "account_type", "type": "STRING"},
    {"name": "balance", "type": "FLOAT"},
    {"name": "status", "type": "STRING"},
    {"name": "created_date", "type": "DATE"},
    {"name": "branch_name", "type": "STRING"}

  ]
}

with beam.Pipeline() as p:
  (
    p
    | "Read Accounts" >>beam.io.ReadFromText("data/accounts.csv",skip_header_lines=1)
    | "parse Accounts" >> beam.ParDo(ParseAccount())
    | "Print Valid Records" >> beam.ParDo(ValidateAccounts())
)   | "Write To BigQuery" >> WriteToBigQuery(table=f"{PROJECT_ID}:{DATASET_ID}.{TABLE_ID}",schema=table_schema, custom_gcs_temp_location=f"gs://{BUCKET_NAME}/temp",write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)
