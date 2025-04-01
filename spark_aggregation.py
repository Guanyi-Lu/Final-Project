#read the non-aggregated data in the bucket and create a table in BigQuery with the following columns:

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

spark = SparkSession.builder \
        .appName('who_mortality_aggregation') \
        .config('temporaryGcsBucket', 'dataproc-temp-us-central1-820191181720-nh4saied') \
        .getOrCreate()

#PROCESSED_BUCKET = "parquet_data_bucket" 
#BQ_DATASET="oval-sunset-455016-n0.Australia_Mortality_Dataset"

def aggregate_data_bq():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    print(f"Input Path: {args.input}")
    print(f"Output Table: {args.output}")


    # Read data from GCS
    df = spark.read.parquet(args.input)
    df.createOrReplaceTempView("non_aggregated_data")

    # Execute your SQL transformation
    df_aggregated = spark.sql("""
    WITH t1 AS (
        SELECT 
            CAST(Year AS INT) AS Year,
            age_group,
            Sex,
            country_name,
            icd_code,
            List,
            CASE
                WHEN LENGTH(CAST(Cause AS STRING)) = 4 THEN CONCAT(SUBSTRING(CAST(Cause AS STRING), 1, 3), '.', SUBSTRING(CAST(Cause AS STRING), 4, 1))
                ELSE CAST(Cause AS STRING)
            END AS Cause_code,
            death_count,
            population
        FROM non_aggregated_data
        WHERE country_name = 'Australia'
    )
    SELECT 
        t1.Year,
        t1.Sex,
        CASE
            WHEN t1.age_group IN ("3", "4", "5", "6", "7") THEN "kid-before-10"
            WHEN t1.age_group IN ("8", "9") THEN "teenager-10-19"
            WHEN t1.age_group IN ("10", "11") THEN "young-adult-20-29"
            WHEN t1.age_group IN ("12", "13", "14", "15", "16") THEN "adult-30-54"
            WHEN t1.age_group IN ("17", "18", "19", "20", "21") THEN "elder-55-79"
            ELSE "old-80+"
        END AS age_group_category,
        t1.Cause_code,
        (SUM(t1.death_count) / SUM(t1.population) * 100) AS death_percentage
    FROM t1
    WHERE t1.Cause_code != "AAA"
    GROUP BY 
        t1.Year, t1.Sex, age_group_category, t1.Cause_code
    ORDER BY t1.Year, t1.Sex, age_group_category, t1.Cause_code
    """)
    
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# Define the schema
    schema = StructType([
        StructField("Year", IntegerType(), True),  # Year is an integer
        StructField("Sex", StringType(), True),  # Sex is a string
        StructField("age_group_category", StringType(), True),  # age_group_category is a string
        StructField("Cause_code", StringType(), True),  # Cause_code is a string
        StructField("death_percentage", FloatType(), True)  # death_percentage is a float
    ])
    
    df_aggregated = spark.createDataFrame(df_aggregated.rdd, schema)

    # Write to BigQuery 
    df_aggregated.write.format('bigquery') \
        .option('table', args.output) \
        .mode('overwrite') \
        .save()



def main():
    aggregate_data_bq()


if __name__ == "__main__":
    main()
    spark.stop()

