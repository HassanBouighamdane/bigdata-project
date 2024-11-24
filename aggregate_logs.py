#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, col
import os
from datetime import datetime
import glob


def create_spark_session():
    """Create a Spark session"""
    return SparkSession.builder \
        .appName("LogProcessing") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .getOrCreate()


def get_hour_folders(base_path):
    """Get all hourly folders"""
    # Folder names are in the format YYYYMMDDHH
    return [d for d in os.listdir(base_path) if len(d) == 10 and d.isdigit()]


def process_log_file(spark, file_path):
    """Process an individual log file"""
    try:
        # Read the file with Spark
        df = spark.read.option("delimiter", "|").csv(file_path)
        
        # Rename columns to match log format
        df = df.toDF("timestamp", "id", "product_name", "price")
        
        # Convert price to float and group by product
        df = df.withColumn("price", col("price").cast("float"))
        
        # Group by product and sum prices
        result_df = df.groupBy("product_name").agg(
            _sum("price").alias("total_price")
        )
        
        return result_df
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")
        return None


def format_datetime(folder_name):
    """Format folder name as a readable date"""
    # Convert YYYYMMDDHH to YYYY/MM/DD HH
    year = folder_name[:4]
    month = folder_name[4:6]
    day = folder_name[6:8]
    hour = folder_name[8:10]
    return f"{year}/{month}/{day} {hour}"


def main():
    # Paths
    input_path = "./logs"  # Change this to your actual logs path
    output_path = "./output"  # Change this to your desired output path
    
    # Create the output directory if it doesn't exist
    os.makedirs(output_path, exist_ok=True)
    
    # Initialize Spark
    spark = create_spark_session()
    
    try:
        # Get all hourly folders
        hour_folders = get_hour_folders(input_path)
        
        for folder in hour_folders:
            folder_path = os.path.join(input_path, folder)
            
            # Get all log files in the folder (format: YYYYMMDDHHMMSS.txt)
            log_files = glob.glob(os.path.join(folder_path, "*.txt"))
            
            # Dictionary to store results for the hour
            hour_results = {}
            
            # Process each log file for the hour
            for file_path in log_files:
                result_df = process_log_file(spark, file_path)
                
                if result_df:
                    # Collect results
                    for row in result_df.collect():
                        product = row.product_name
                        amount = row.total_price  # Already in float
                        
                        if product not in hour_results:
                            hour_results[product] = 0.0
                        hour_results[product] += amount
            
            # Write results to a file for the hour
            if hour_results:
                output_file = os.path.join(output_path, f"{folder}.txt")
                date_str = format_datetime(folder)
                
                with open(output_file, 'a') as f:
                    for product, total in hour_results.items():
                        f.write(f"{date_str}|{product}|{total:.2f}\n")
                
                print(f"File created: {output_file}")
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
