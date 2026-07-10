import apache_beam as beam
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

with beam.Pipeline() as p:
  (
    p
    | "Read Accounts" >>beam.io.ReadFromText("data/accounts.csv",skip_header_lines=1)
    | "parse Accounts" >> beam.ParDo(ParseAccount())
    | "Print Valid Records" >> beam.ParDo(ValidateAccounts())
)   | "Write Valid Records" >> beam.io.WriteToText("data/accounts_valid.v2")
