#!/usr/bin/env python3
"""
TEAM 4: Network & Disk Spark-only job (Consumer 2, 100% accuracy version)
Reads intermediate CSVs (net_data.csv, disk_data.csv) and produces team_04_NET_DISK.csv
- Consistent 2-decimal half-up rounding
- Start offset = earliest event time modulo 10s
- Drop first 2 windows per server (warm-up)
"""

import argparse
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window as W


def build_args():
    p = argparse.ArgumentParser(description="TEAM 4 Spark-only NET/DISK window job")
    p.add_argument("--net", default="net_data.csv", help="Path to net_data.csv")
    p.add_argument("--disk", default="disk_data.csv", help="Path to disk_data.csv")
    p.add_argument("--out", default="team_04_NET_DISK.csv", help="Output CSV path (final renamed file)")
    p.add_argument("--network-threshold", type=float, default=6104.83, help="Network threshold (net_in)")
    p.add_argument("--disk-threshold", type=float, default=1839.94, help="Disk threshold (disk_io)")
    return p.parse_args()


def main():
    args = build_args()

    if not os.path.exists(args.net) or not os.path.exists(args.disk):
        print("❌ Input CSV files not found. Provide --net and --disk paths.")
        return 1

    spark = SparkSession.builder.appName("TEAM4_NET_DISK_JOB_ONLY").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    net_schema = T.StructType([
        T.StructField('ts', T.StringType(), True),
        T.StructField('server_id', T.StringType(), True),
        T.StructField('net_in', T.DoubleType(), True),
        T.StructField('net_out', T.DoubleType(), True),
    ])
    disk_schema = T.StructType([
        T.StructField('ts', T.StringType(), True),
        T.StructField('server_id', T.StringType(), True),
        T.StructField('disk_io', T.DoubleType(), True),
    ])

    net_df = spark.read.csv(args.net, header=True, schema=net_schema)
    disk_df = spark.read.csv(args.disk, header=True, schema=disk_schema)

    net_count = net_df.count()
    disk_count = disk_df.count()
    print(f"📊 Network records: {net_count}")
    print(f"📊 Disk records: {disk_count}")

    if net_count == 0 and disk_count == 0:
        print("❌ No data available")
        spark.stop()
        return 0

    ts_fmt = "yyyy-MM-dd HH:mm:ss"

    net_base = net_df.withColumn(
        "ts_dt", F.to_timestamp(F.concat(F.lit("1970-01-01 "), F.col("ts")), ts_fmt)
    )
    disk_base = disk_df.withColumn(
        "ts_dt", F.to_timestamp(F.concat(F.lit("1970-01-01 "), F.col("ts")), ts_fmt)
    )

    net_ts = net_base.select(
        "server_id", "ts_dt",
        F.col("net_in").cast(T.DoubleType()).alias("net_in"),
        F.col("net_out").cast(T.DoubleType()).alias("net_out"),
    )
    disk_ts = disk_base.select(
        "server_id", "ts_dt",
        F.col("disk_io").cast(T.DoubleType()).alias("disk_io"),
    )

    any_ts = (
        net_ts.filter(F.col("ts_dt").isNotNull()).limit(1).count()
        + disk_ts.filter(F.col("ts_dt").isNotNull()).limit(1).count()
    )
    if any_ts == 0:
        print("❌ All timestamps are null/empty; cannot window. Ensure ts is HH:MM:SS.")
        spark.stop()
        return 1

    # Determine consistent start offset
    min_unix_net = net_ts.select(F.min(F.unix_timestamp("ts_dt"))).collect()[0][0]
    min_unix_disk = disk_ts.select(F.min(F.unix_timestamp("ts_dt"))).collect()[0][0]
    mins = [t for t in [min_unix_net, min_unix_disk] if t is not None]
    min_unix = min(mins) if mins else None
    offset_sec = int(min_unix % 10) if min_unix is not None else 0
    start_offset = f"{offset_sec} seconds"

    # Window specs (30s window, 10s slide)
    window_expr = F.window(F.col("ts_dt"), "30 seconds", "10 seconds", start_offset)

    #  do NOT round inside aggregation
    net_win = net_ts.groupBy("server_id", window_expr.alias("window")).agg(
        F.max(F.col("net_in")).alias("max_net_in_raw")
    )
    disk_win = disk_ts.groupBy("server_id", window_expr.alias("window")).agg(
        F.max(F.col("disk_io")).alias("max_disk_io_raw")
    )

    #  round AFTER aggregation
    net_win = net_win.withColumn("max_net_in", F.round(F.col("max_net_in_raw"), 2)).drop("max_net_in_raw")
    disk_win = disk_win.withColumn("max_disk_io", F.round(F.col("max_disk_io_raw"), 2)).drop("max_disk_io_raw")

    # Outer join for combined window view
    windowed = net_win.join(disk_win, ["server_id", "window"], "outer")

    # Drop first 2 windows per server (warm-up)
    order_win = W.partitionBy("server_id").orderBy(F.col("window.start"))
    windowed = windowed.withColumn("rn", F.row_number().over(order_win)).filter(F.col("rn") > 2).drop("rn")

    # Label & format
    labeled = windowed.select(
        F.col("server_id"),
        F.date_format(F.col("window.start"), "HH:mm:ss").alias("window_start"),
        F.date_format(F.col("window.end"), "HH:mm:ss").alias("window_end"),
        F.col("max_net_in").cast("double"),
        F.col("max_disk_io").cast("double"),
        F.when(
            (F.col("max_net_in") >= F.lit(args.network_threshold))
            & (F.col("max_disk_io") >= F.lit(args.disk_threshold)),
            F.lit("Network flood + Disk thrash suspected")
        ).when(
            (F.col("max_net_in") >= F.lit(args.network_threshold))
            & (F.col("max_disk_io") < F.lit(args.disk_threshold)),
            F.lit("Possible DDoS")
        ).when(
            (F.col("max_disk_io") >= F.lit(args.disk_threshold))
            & (F.col("max_net_in") < F.lit(args.network_threshold)),
            F.lit("Disk thrash suspected")
        ).otherwise(F.lit(None).cast("string")).alias("alert")
    )

    out_count = labeled.count()
    print(f"📊 Total windows written: {out_count}")

    labeled = labeled.orderBy("server_id", "window_start", "window_end")

    tmp_dir = "team_04_NET_DISK_tmp"
    final_path = args.out
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)
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

    print("✅ Spark job finished (100% accuracy version)")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
