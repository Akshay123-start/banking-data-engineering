import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from configs.config import PROJECT_ID, DATASET_ID, TRANSACTION_TABLE_ID


class ParseTransaction(beam.DoFn):
    def process(self, line):

        fields = line.split(",")

        yield {
            "transaction_id": fields[0],
            "account_id": fields[1],
            "amount": float(fields[2]),
            "transaction_type": fields[3],
            "transaction_date": fields[4],
            "status": fields[5]
        }


class ValidationTransaction(beam.DoFn):
    def process(self, record):

        if (
            record["amount"] > 0
            and record["transaction_type"] in ["Deposit", "Withdrawal"]
            and record["status"] in ["Success", "Failed"]
        ):
            yield record


table_schema = {
    "fields": [
        {"name": "transaction_id", "type": "STRING"},
        {"name": "account_id", "type": "STRING"},
        {"name": "amount", "type": "FLOAT"},
        {"name": "transaction_type", "type": "STRING"},
        {"name": "transaction_date", "type": "DATE"},
        {"name": "status", "type": "STRING"}
    ]
}


with beam.Pipeline() as p:
    (
        p
        | "Read Transactions" >> beam.io.ReadFromText(
            "data/transactions.csv",
            skip_header_lines=1
        )
        | "Parse Transactions" >> beam.ParDo(ParseTransaction())
        | "Validate Transactions" >> beam.ParDo(ValidationTransaction())
        | "Write To BigQuery" >> WriteToBigQuery(
            table=f"{PROJECT_ID}:{DATASET_ID}.{TRANSACTION_TABLE_ID}",
            schema=table_schema,
            custom_gcs_temp_location="gs://akshay-data-engineer-2026/temp",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        )
    )