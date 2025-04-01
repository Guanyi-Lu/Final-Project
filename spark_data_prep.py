from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, regexp_replace, col,when
import pandas as pd
from google.cloud import storage
import zipfile
import io
import os
from pyspark.sql import DataFrame
from functools import reduce
from google.cloud import storage


# Initialize Spark with BigQuery support
#spark = SparkSession.builder \
    #.appName("WHO Mortality Data Processing") \
    #.config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.23.2") \
    #.getOrCreate()

#the memory and cores set up is important, otherwise it will fail due to memory issue
spark = SparkSession.builder \
    .appName("WHO Mortality Data Processing") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
# GCS paths
RAW_BUCKET = "raw_bucket_guanyi"
PROCESSED_BUCKET = "parquet_data_bucket"  


def process_zip_files():
    """
    1. Extracts all files from ZIPs in GCS raw bucket
    2. Converts .file to .csv
    3. Uploads all files to processed bucket
    """
    storage_client = storage.Client()
    raw_bucket = storage_client.bucket(RAW_BUCKET)
    processed_bucket = storage_client.bucket(PROCESSED_BUCKET)

    # Process each ZIP file in raw bucket
    for blob in raw_bucket.list_blobs(prefix="raw/"):
        if not blob.name.endswith('.zip'):
            continue  # Skip non-ZIP files

        print(f"\nProcessing {blob.name}...")

        # Download ZIP to memory
        zip_bytes = io.BytesIO(blob.download_as_bytes())

        with zipfile.ZipFile(zip_bytes) as zip_ref:
            for file_info in zip_ref.infolist():
                # Skip directories and macOS metadata
                if file_info.is_dir() or '__MACOSX' in file_info.filename:
                    continue

                original_name = file_info.filename.split('/')[-1]  # Remove path
                print(f"  Found file: {original_name}")

                # Read file content
                with zip_ref.open(file_info) as file:
                    file_content = file.read()

                # Process based on file type
                if original_name.endswith('.file'):
                    # Convert .file to CSV
                    csv_name = original_name.replace('.file', '.csv')
                    
                    try:
                        # If it's a text file, decode and write as CSV
                        try:
                            text_content = file_content.decode('utf-8')
                            processed_bucket.blob(f"extracted/{csv_name}").upload_from_string(text_content)
                            print(f"    Converted {original_name} → extracted/{csv_name}")
                        except UnicodeDecodeError:
                            # If binary, write as-is with CSV extension
                            processed_bucket.blob(f"extracted/{csv_name}").upload_from_string(file_content)
                            print(f"    Saved binary {original_name} as extracted/{csv_name}")
                    
                    except Exception as e:
                        print(f"    Failed to process {original_name}: {str(e)}")
                        
                else:
                    # Upload other files (.xlsx, .pdf) as-is
                    processed_bucket.blob(f"extracted/{original_name}").upload_from_string(file_content)
                    print(f"    Copied {original_name} to extracted/")

    print("\nAll files processed successfully!")

def move_non_excel_pdf_files():
    """Move files from EXTRACTED bucket to another"""
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(PROCESSED_BUCKET)
    dest_bucket = storage_client.bucket(PROCESSED_BUCKET)
    formats = ('.xlsx', '.pdf')
    for blob in source_bucket.list_blobs(prefix="extracted/"):
        if blob.name.endswith('/') or blob.name.lower().endswith(formats):
            continue 

        filename= blob.name.split('/')[-1]   # Get the filename without path
        new_blob_name = f"file_for_process/{filename}"
        dest_blob = dest_bucket.blob(new_blob_name)
        dest_blob.rewrite(blob)
        print(f"Moved {blob.name} to {new_blob_name}")





def process_with_spark():
    """Main PySpark processing pipeline for WHO mortality data"""
    try:
        # Columns to drop from all datasets
        columns_to_drop = ['Admin1', 'SubDiv', 'Frmat', 'IM_Frmat']
        
        # ===== 1. PROCESS ICD-10 FILES (PARTS 1-6) =====
        dfs_10 = []
        for i in range(1, 7):
            path = f"gs://{PROCESSED_BUCKET}/file_for_process/Morticd10_part{i}"
            try:
                df = spark.read.csv(path, header=True)
                # Drop unnecessary columns immediately after reading
                df = df.drop(*columns_to_drop).withColumn("icd_code", lit(10).cast("integer"))
                dfs_10.append(df)
                print(f"Successfully processed ICD-10 Part {i}")
            except Exception as e:
                print(f"Could not read {path}: {str(e)}")
                continue

        if not dfs_10:
            raise ValueError("No ICD-10 files were processed successfully")
        
        df_10 = reduce(DataFrame.unionAll, dfs_10)

        # ===== 2. PROCESS OTHER ICD VERSIONS =====
        def load_icd_file(version, exact_filename):
            """Helper to load specific ICD files with exact naming"""
            path = f"gs://{PROCESSED_BUCKET}/file_for_process/{exact_filename}"
            try:
                df = spark.read.csv(path, header=True)
                # Drop unnecessary columns immediately
                return df.drop(*columns_to_drop).withColumn("icd_code", lit(version).cast("integer"))
            except Exception as e:
                print(f"Error processing {path}: {str(e)}")
                raise

        # Process with exact filenames
        df_7 = load_icd_file(7, "MortIcd7")  # Uppercase I
        df_8 = load_icd_file(8, "Morticd8")  # Lowercase i
        df_9 = load_icd_file(9, "Morticd9")  # Lowercase i

        # ===== 3. UNPIVOT ALL ICD DATA =====
        def unpivot_icd_data(df):
            # Updated static cols after dropping unnecessary columns
            static_cols = ['Country', 'Year', 'List', 'Cause', 'Sex', 'icd_code']
            death_cols = [f"Deaths{i}" for i in range(1, 27)]
            infant_cols = [f"IM_Deaths{i}" for i in range(1, 5)]
            
            return (df.select(static_cols + death_cols + infant_cols)
                    .unpivot(static_cols, death_cols + infant_cols, "age_group", "death_count")
                    .withColumn("age_group", 
                              when(col("age_group").startswith("IM_"), 
                                   regexp_replace(col("age_group"), "IM_Deaths", "IM"))
                              .otherwise(regexp_replace(col("age_group"), "Deaths", "")))
                   )

        df_10 = unpivot_icd_data(df_10)
        df_7 = unpivot_icd_data(df_7)
        df_8 = unpivot_icd_data(df_8)
        df_9 = unpivot_icd_data(df_9)

        # ===== 4. COMBINE ALL ICD DATA =====
        final_icd_df = df_10.unionByName(df_9).unionByName(df_8).unionByName(df_7)

        # ===== 5. PROCESS POPULATION DATA =====
        pop_path = f"gs://{PROCESSED_BUCKET}/file_for_process/pop"
        df_pop = spark.read.csv(pop_path, header=True)
        
        # Drop same unnecessary columns from population data
        df_pop = df_pop.drop(*columns_to_drop)
        pop_static = ['Country', 'Year', 'Sex']  # Updated after dropping columns
        pop_cols = [f"Pop{i}" for i in range(1, 27)]
        
        df_pop = (df_pop.unpivot(pop_static, pop_cols, "age_group", "population")
                  .withColumn("age_group", regexp_replace(col("age_group"), "Pop", "")))

        # ===== 6. PROCESS COUNTRY CODES =====
        country_path = f"gs://{PROCESSED_BUCKET}/file_for_process/country_codes"
        df_country = (spark.read.csv(country_path, header=True)
                      .withColumnRenamed("country", "Country")
                      .withColumnRenamed("name", "country_name"))

        # ===== 7. JOIN ALL DATASETS =====
        # Updated join columns after dropping Admin1, SubDiv
        join_columns = ["Country", "Year", "Sex", "age_group"]
        df_final = (final_icd_df.join(df_pop, join_columns, "inner")
                              .join(df_country, "Country", "inner"))
        
     
        print("\n=== VERIFYING FINAL DATAFRAME ===")
        print("Sample of final joined data (first 5 rows):")
        df_final.show(5, truncate=False)

        # ===== 8. FILTER AUSTRALIA DATA =====
        df_Australia = df_final.filter(
        (col("Country") == "5020") & 
        (col("Year").isin(['2010', '2011', '2012', '2013', '2014', '2015']) )  )
             
        print("\n=== VERIFYING Aus DATA ===")
        print("Sample of Australia data (first 5 rows):")
        
        df_Australia .show(5, truncate=False)
        
        if  df_Australia .isEmpty():
            raise ValueError("No Australia data found after filtering!")
        
        # ===== 9. SAVE RESULTS =====
        output_path = f"gs://{PROCESSED_BUCKET}/australia_data"
        ( df_Australia.repartition(4)
                     .write
                     .mode("overwrite")
                     .parquet(output_path))
        
        print(f"Successfully processed and saved Australia data to {output_path}")
        return True

    except Exception as e:
        print(f"Fatal error in processing pipeline: {str(e)}")
        raise
    finally:
        spark.stop()








# Add this at the end of spark_data_prep_test.py (outside functions)
def main():
    process_zip_files()
    move_non_excel_pdf_files()  # Now properly called
    process_with_spark()

if __name__ == "__main__":
    main()  # For local testing
    spark.stop()