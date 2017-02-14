bq rm -f nyt_singlecopy.singlecopy_bypass_all
bq mk --schema singlecopy_bypass.json -t nyt_singlecopy.singlecopy_bypass_all
bq load --skip_leading_rows=1  nyt_singlecopy.singlecopy_bypass_all singlecopy_bypass.csv
