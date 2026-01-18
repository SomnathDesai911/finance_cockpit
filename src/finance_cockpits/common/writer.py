from pyspark.sql import SparkSession, DataFrame

def write_data(
    df:DataFrame,
    table_path: str,
    format="delta",
    write_mode= "overwrite"
):

    # By default write data to delta format, provide option to write to other formats

    (   df.write
        .format(format)
        .mode(write_mode)
        .saveAsTable(table_path))

    print(f"Data writtern successfully to path:{table_path}")