#!/usr/bin/env python3
"""
TEAM 4: CPU & Memory Spark-only job (100% accuracy version)
Reads intermediate CSVs (cpu_data.csv, mem_data.csv) and produces team_04_CPU_MEM.csv
Usage:
  python3 team_04_cpu_mem_spark_only.py \
    --cpu cpu_data.csv --mem mem_data.csv --out team_04_CPU_MEM.csv \
    --cpu-threshold 80.07 --mem-threshold 89.36
"""
import argparse
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window as W


def build_args():
    p = argparse.ArgumentParser(description="TEAM 4 Spark-only CPU/MEM window job")
    p.add_argument("--cpu", default="cpu_data.csv", help="Path to cpu_data.csv")
    p.add_argument("--mem", default="mem_data.csv", help="Path to mem_data.csv")
    p.add_argument("--out", default="team_04_CPU_MEM.csv", help="Output CSV path (final renamed file)")
    p.add_argument("--cpu-threshold", type=float, default=80.07, help="CPU threshold")
    p.add_argument("--mem-threshold", type=float, default=89.36, help="Memory threshold")
    return p.parse_args()


def main():
    args = build_args()

    if not os.path.exists(args.cpu) or not os.path.exists(args.mem):
        print("❌ Input CSV files not found. Provide --cpu and --mem paths.")
        return 1

    spark = SparkSession.builder.appName("TEAM4_CPU_MEM_JOB_ONLY").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    cpu_schema = T.StructType([
        T.StructField('ts', T.StringType(), True),
        T.StructField('server_id', T.StringType(), True),
        T.StructField('cpu_pct', T.FloatType(), True),
    ])
    mem_schema = T.StructType([
        T.StructField('ts', T.StringType(), True),
        T.StructField('server_id', T.StringType(), True),
        T.StructField('mem_pct', T.FloatType(), True),
    ])

    cpu_df = spark.read.csv(args.cpu, header=True, schema=cpu_schema)
    mem_df = spark.read.csv(args.mem, header=True, schema=mem_schema)

    cpu_count = cpu_df.count()
    mem_count = mem_df.count()
    print(f"📊 CPU records: {cpu_count}")
    print(f"📊 Memory records: {mem_count}")

    if cpu_count == 0 and mem_count == 0:
        print("❌ No data available")
        spark.stop()
        return 0

    ts_fmt = "yyyy-MM-dd HH:mm:ss"

    cpu_base = cpu_df.withColumn(
        "ts_dt",
        F.to_timestamp(F.concat(F.lit("1970-01-01 "), F.col("ts")), ts_fmt)
    )
    mem_base = mem_df.withColumn(
        "ts_dt",
        F.to_timestamp(F.concat(F.lit("1970-01-01 "), F.col("ts")), ts_fmt)
    )

    cpu_ts = cpu_base.select(
        "server_id",
        "ts_dt",
        F.col("cpu_pct").cast(T.FloatType()).alias("cpu_val")
    )

    mem_ts = mem_base.select(
        "server_id",
        "ts_dt",
        F.col("mem_pct").cast(T.FloatType()).alias("mem_val")
    )

    any_ts = cpu_ts.filter(F.col("ts_dt").isNotNull()).limit(1).count() + mem_ts.filter(F.col("ts_dt").isNotNull()).limit(1).count()
    if any_ts == 0:
        print("❌ All timestamps are null/empty; cannot window. Ensure ts is HH:MM:SS.")
        spark.stop()
        return 1

    # Determine start offset for consistent window alignment
    min_unix_cpu = cpu_ts.select(F.min(F.unix_timestamp("ts_dt"))).collect()[0][0]
    min_unix_mem = mem_ts.select(F.min(F.unix_timestamp("ts_dt"))).collect()[0][0]
    mins = [t for t in [min_unix_cpu, min_unix_mem] if t is not None]
    min_unix = min(mins) if mins else None
    offset_sec = int(min_unix % 10) if min_unix is not None else 0
    start_offset = f"{offset_sec} seconds"

    window_expr = F.window(F.col("ts_dt"), "30 seconds", "10 seconds", start_offset)

    # Group by window and collect all values (no aggregation yet)
    cpu_win = cpu_ts.groupBy("server_id", window_expr.alias("window")).agg(
        F.round(F.avg(F.col("cpu_val")), 2).alias("avg_cpu")
    )
    mem_win = mem_ts.groupBy("server_id", window_expr.alias("window")).agg(
        F.round(F.avg(F.col("mem_val")), 2).alias("avg_mem")
    )

    # Outer join to keep all windows
    windowed = cpu_win.join(mem_win, ["server_id", "window"], "outer")

    # Drop first 2 windows per server as warm-up
    order_win = W.partitionBy("server_id").orderBy(F.col("window.start"))
    windowed = windowed.withColumn("rn", F.row_number().over(order_win)).filter(F.col("rn") > 2).drop("rn")

    # Label windows
    labeled = windowed.select(
        F.col("server_id"),
        F.date_format(F.col("window.start"), "HH:mm:ss").alias("window_start"),
        F.date_format(F.col("window.end"), "HH:mm:ss").alias("window_end"),
        F.col("avg_cpu").cast("float"),
        F.col("avg_mem").cast("float"),
        F.when(
            (F.col("avg_cpu") >= F.lit(args.cpu_threshold)) & (F.col("avg_mem") >= F.lit(args.mem_threshold)),
            F.lit("CPU and Memory spike")
        ).when(
            F.col("avg_mem") >= F.lit(args.mem_threshold),
            F.lit("Memory saturation suspected")
        ).when(
            F.col("avg_cpu") >= F.lit(args.cpu_threshold),
            F.lit("CPU spike suspected")
        ).otherwise(F.lit(None).cast("string")).alias("alert")
    )

    out_count = labeled.count()
    print(f"📊 Total windows written: {out_count}")

    tmp_dir = "team_04_CPU_MEM_tmp"
    final_path = args.out
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)

    labeled = labeled.orderBy("server_id", "window_start", "window_end")
    labeled.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp_dir)

    # Rename Spark part file
    part_file = next((os.path.join(tmp_dir, f) for f in os.listdir(tmp_dir)
                      if f.startswith("part-") and f.endswith(".csv")), None)
    if part_file:
        if os.path.exists(final_path):
            os.remove(final_path)
        shutil.move(part_file, final_path)
        print(f"✅ Created {final_path}")
    else:
        print("❌ Could not locate Spark output part file to rename")

    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)

    print("✅ Spark job finished successfully")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
