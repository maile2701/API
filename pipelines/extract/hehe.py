import pandas as pd
import csv

input_file = "/Users/thanhmai/etl_pipeline test/data/person_cleaned2.csv"
output_file = "/Users/thanhmai/etl_pipeline test/data/person_cleaned.csv"

df = pd.read_csv(input_file, sep=";")

df.to_csv(output_file, index=False, sep=",", quoting=csv.QUOTE_MINIMAL, encoding="utf-8")
