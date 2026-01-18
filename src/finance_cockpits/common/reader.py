from pyspark.sql import SparkSession, DataFrame

def read_excel(
    spark:SparkSession,
    file_path: str,
    sheet_name: str = "",
    header_Rows=1,
    infer_schema: bool = True,
    data_address: str = None
):

    # Default to full sheet starting at A1
    if not data_address:
        data_address = sheet_name

    df = (
        spark.read
        .format("excel")
        .option("headerRows", header_Rows)
        .option("inferSchema", str(infer_schema).lower())
        .option("dataAddress", data_address)
        .load(file_path)
    )

    return df


def read_delta_table(
    spark:DataFrame,
    table_path
    ):

    df=(spark.table(table_path))

    return df

