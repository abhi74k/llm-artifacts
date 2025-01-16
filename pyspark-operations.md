# PySpark Operations Guide: Detailed Examples with Market Data

## Table of Contents
1. Row-Level Operations
   - filter()
   - withColumn()
2. Streaming Operations
   - withWatermark()
3. Aggregation Operations
   - groupBy()
   - agg()
   - Aggregation Functions
4. Conditional Operations
   - when()
   - first()
5. Windowing Operations
   - Window.partitionBy()
   - orderBy()
   - rowsBetween()
6. Advanced Operations
   - pivot()
   - lag()
   - lit()

## 1. Row-Level Operations

### filter()
The `filter()` operation (also known as `where()`) filters rows based on a condition.

```python
from pyspark.sql.functions import col

# Simple AND/OR combination
df_filtered = trades_df.filter(
    (col("price") > 100) & 
    ((col("exchange") == "NYSE") | (col("exchange") == "NASDAQ"))
)

# Complex AND/OR nesting
df_filtered = trades_df.filter(
    # Main condition group 1: price-related
    ((col("price") > 100) & (col("volume") >= 1000)) |
    # Main condition group 2: exchange-related
    ((col("exchange").isin(["NYSE", "NASDAQ"])) & (col("volume") < 500))
)

# Multiple AND/OR groups with clear grouping
df_filtered = trades_df.filter(
    # High value trades on major exchanges
    ((col("price") > 500) & (col("exchange").isin(["NYSE", "NASDAQ"]))) |
    # OR medium value trades with large volume
    ((col("price") > 100) & (col("price") <= 500) & (col("volume") > 10000)) |
    # OR any dark pool trades
    (col("exchange") == "DARK")
)

# SQL-style mixed conditions
df_filtered = trades_df.filter("""
    (price > 100 AND volume >= 1000) OR
    (exchange IN ('NYSE', 'NASDAQ') AND volume < 500) OR
    (trade_type = 'BLOCK' AND price > 200)
""")

# String filters using AND, OR, NOT
df_filtered = trades_df.filter("""
    (exchange = 'NYSE' OR exchange = 'NASDAQ')
    AND NOT (price < 100 OR volume < 1000)
    AND trade_type NOT IN ('DARK', 'OTC')
""")

# Complex string conditions with multiple NOT
df_filtered = trades_df.filter("""
    (price > 100 AND NOT exchange = 'OTC')
    OR (volume >= 1000 AND NOT price < 50)
    OR (NOT exchange IN ('DARK', 'OTC') AND NOT trade_type = 'BLOCK')
""")

# String conditions with NULL handling
df_filtered = trades_df.filter("""
    (price IS NOT NULL AND volume > 0)
    AND (exchange IS NOT NULL AND exchange != '')
    AND NOT (trade_type IS NULL OR trade_type = '')
""")

# Using column aliases for readability
# Calculate quantiles for filtering
from pyspark.sql.functions import percent_rank

# Create volume quantile column
window_spec = Window.partitionBy("symbol").orderBy("volume")
trades_with_quantiles = trades_df.withColumn(
    "volume_percentile", 
    percent_rank().over(window_spec)
)

# Filter based on quantile
is_major_exchange = col("exchange").isin(["NYSE", "NASDAQ"])
is_high_price = col("price") > 100
is_large_volume = col("volume_percentile") >= 0.75  # Top 25% of volume

df_filtered = trades_with_quantiles.filter(
    (is_high_price & is_large_volume) |
    (is_major_exchange & ~is_large_volume)  # ~ for NOT
)

# Pivot by volume quantile buckets
from pyspark.sql.functions import ntile, avg, stddev

# Create volume buckets (quartiles)
window_spec = Window.partitionBy("symbol").orderBy("volume")
trades_bucketed = trades_df.withColumn(
    "volume_bucket", 
    ntile(4).over(window_spec)
)

# Pivot and calculate metrics by bucket
pivot_metrics = trades_bucketed \
    .groupBy("symbol") \
    .pivot("volume_bucket", [1, 2, 3, 4]) \
    .agg(
        avg("return").alias("avg_return"),
        stddev("return").alias("return_volatility"),
        count("*").alias("trade_count")
    )

# Calculate forward and backward markouts
window_spec = Window.partitionBy("symbol").orderBy("timestamp")

markouts_df = trades_df \
    .withColumn("price_change_5min_fwd", 
        (lead("price", 1).over(window_spec) - col("price")) / col("price")) \
    .withColumn("price_change_10min_fwd", 
        (lead("price", 2).over(window_spec) - col("price")) / col("price")) \
    .withColumn("price_change_5min_bwd", 
        (col("price") - lag("price", 1).over(window_spec)) / lag("price", 1).over(window_spec)) \
    .withColumn("price_change_10min_bwd", 
        (col("price") - lag("price", 2).over(window_spec)) / lag("price", 2).over(window_spec))

# Analyze markouts by trade size quantile
markouts_analysis = markouts_df \
    .withColumn("volume_bucket", ntile(4).over(Window.partitionBy("symbol").orderBy("volume"))) \
    .groupBy("symbol", "volume_bucket") \
    .agg(
        avg("price_change_5min_fwd").alias("avg_5min_fwd_impact"),
        avg("price_change_10min_fwd").alias("avg_10min_fwd_impact"),
        avg("price_change_5min_bwd").alias("avg_5min_bwd_impact"),
        avg("price_change_10min_bwd").alias("avg_10min_bwd_impact")
    )
)

# Filtering with isin()
valid_exchanges = ['NYSE', 'NASDAQ', 'ARCA']
df_filtered = trades_df.filter(col("exchange").isin(valid_exchanges))
```

### withColumn()
The `withColumn()` operation adds a new column or replaces an existing one.

```python
from pyspark.sql.functions import col, round, when

# Simple calculation
df = trades_df.withColumn("value", col("price") * col("volume"))

# Multiple transformations
df = trades_df \
    .withColumn("value", col("price") * col("volume")) \
    .withColumn("price_rounded", round(col("price"), 2)) \
    .withColumn("large_trade", when(col("volume") > 10000, True).otherwise(False))

# Derived calculations
df = trades_df.withColumn(
    "price_category",
    when(col("price") < 10, "penny_stock")
    .when(col("price") < 100, "mid_range")
    .otherwise("high_value")
)
```

## 2. Streaming Operations

### withWatermark()
The `withWatermark()` operation is used in structured streaming to handle late data and state cleanup.

```python
from pyspark.sql.functions import window

# Basic watermark
df_stream = trades_df \
    .withWatermark("timestamp", "10 minutes")

# Watermark with window aggregation
df_stream = trades_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window("timestamp", "5 minutes"),
        "symbol"
    ) \
    .agg(avg("price").alias("avg_price"))

# Complex streaming example
df_stream = trades_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window("timestamp", "5 minutes", "1 minute"),  # 5-min window, 1-min sliding
        "symbol"
    ) \
    .agg(
        avg("price").alias("avg_price"),
        sum("volume").alias("total_volume")
    )
```

## 3. Aggregation Operations

### groupBy()
The `groupBy()` operation groups data by one or more columns.

```python
# Simple grouping
df_grouped = trades_df.groupBy("symbol")

# Multiple columns
df_grouped = trades_df.groupBy("symbol", "exchange")

# Time-based grouping
df_grouped = trades_df.groupBy(
    window("timestamp", "5 minutes"),
    "symbol"
)

# Complex grouping with expressions
from pyspark.sql.functions import date_trunc
df_grouped = trades_df.groupBy(
    date_trunc("hour", col("timestamp")).alias("hour"),
    "symbol"
)
```

### agg()
The `agg()` operation performs aggregations on grouped data.

```python
# Basic aggregations
df_agg = trades_df.groupBy("symbol").agg(
    avg("price").alias("avg_price"),
    sum("volume").alias("total_volume"),
    count("*").alias("trade_count")
)

# Multiple complex aggregations
df_agg = trades_df.groupBy("symbol", "exchange").agg(
    avg("price").alias("avg_price"),
    sum("volume").alias("total_volume"),
    count("*").alias("trade_count"),
    min("price").alias("min_price"),
    max("price").alias("max_price"),
    sum(col("price") * col("volume")).alias("total_value")
)

# Aggregations allowed in agg():
# - count(), sum(), avg(), mean()
# - min(), max()
# - first(), last()
# - collect_list(), collect_set()
# - approx_count_distinct()
# - stddev(), variance()
# - skewness(), kurtosis()
# - percentile(), percentile_approx()
```

## 4. Conditional Operations

### when()
The `when()` operation creates conditional expressions.

```python
# Simple when
df = trades_df.withColumn(
    "trade_size",
    when(col("volume") < 100, "small")
    .when(col("volume") < 1000, "medium")
    .otherwise("large")
)

# Complex conditions
df = trades_df.withColumn(
    "trade_category",
    when(
        (col("volume") > 10000) & (col("price") > 100),
        "large_high_value"
    )
    .when(
        (col("volume") > 10000) & (col("price") <= 100),
        "large_low_value"
    )
    .otherwise("regular")
)

# Nested conditions
df = trades_df.withColumn(
    "trade_flag",
    when(col("exchange") == "NYSE",
        when(col("volume") > 10000, "NYSE_large")
        .when(col("volume") > 1000, "NYSE_medium")
        .otherwise("NYSE_small")
    )
    .otherwise("other_exchange")
)
```

### first()
The `first()` operation returns the first value in a group.

```python
# Basic first
df_first = trades_df.groupBy("symbol").agg(
    first("price").alias("first_price")
)

# Conditional first
df_first = trades_df.groupBy("symbol").agg(
    first("price", ignorenulls=True).alias("first_price"),
    first(
        when(col("volume") > 1000, col("price"))
    ).alias("first_large_trade_price")
)

# Complex first with window
from pyspark.sql.window import Window
window_spec = Window.partitionBy("symbol").orderBy("timestamp")
df = trades_df.withColumn(
    "first_price_of_day",
    first("price").over(window_spec)
)
```

## 5. Windowing Operations

### Window.partitionBy()
Creates a window specification for window operations.

```python
from pyspark.sql.window import Window

# Basic window
window_spec = Window.partitionBy("symbol")

# Window with ordering
window_spec = Window \
    .partitionBy("symbol") \
    .orderBy("timestamp")

# Window with multiple partitions
window_spec = Window \
    .partitionBy("symbol", "exchange") \
    .orderBy("timestamp")
```

### orderBy()
Specifies the ordering within a window or DataFrame.

```python
# Window ordering
window_spec = Window \
    .partitionBy("symbol") \
    .orderBy(
        col("timestamp").asc(),
        col("trade_id").asc()
    )

# Multiple order conditions
window_spec = Window \
    .partitionBy("symbol") \
    .orderBy(
        col("timestamp").asc(),
        col("price").desc(),
        col("volume").desc()
    )
```

### rowsBetween()
Defines the window frame for calculations.

```python
# Rolling window calculation
window_spec = Window \
    .partitionBy("symbol") \
    .orderBy("timestamp") \
    .rowsBetween(-9, 0)  # 10-row rolling window

# Calculate 10-period moving average
df = trades_df.withColumn(
    "moving_avg_price",
    avg("price").over(window_spec)
)

# Complex window frame
window_spec = Window \
    .partitionBy("symbol") \
    .orderBy("timestamp") \
    .rowsBetween(
        Window.unboundedPreceding,  # All previous rows
        Window.currentRow           # Up to current row
    )
```

## 6. Advanced Operations

### pivot()
Transforms rows into columns.

```python
# Basic pivot
df_pivot = order_book_df \
    .groupBy("symbol", "timestamp") \
    .pivot("side", ["BID", "ASK"]) \
    .agg(first("price").alias("price"))

# Complex pivot with multiple aggregations
df_pivot = order_book_df \
    .groupBy("symbol") \
    .pivot("exchange", ["NYSE", "NASDAQ", "ARCA"]) \
    .agg(
        avg("price").alias("avg_price"),
        sum("volume").alias("total_volume")
    )
```

### lag()
Accesses previous rows in a window.

```python
# Calculate returns
window_spec = Window \
    .partitionBy("symbol") \
    .orderBy("timestamp")

df = trades_df.withColumn(
    "return",
    (col("price") - lag("price", 1).over(window_spec)) / 
    lag("price", 1).over(window_spec)
)

# Multiple periods
df = trades_df \
    .withColumn("prev_price_1", lag("price", 1).over(window_spec)) \
    .withColumn("prev_price_5", lag("price", 5).over(window_spec)) \
    .withColumn("prev_price_10", lag("price", 10).over(window_spec))
```

### lit()
Creates a literal column value.

```python
# Simple literal
df = trades_df.withColumn("constant", lit(1))

# Used in calculations
df = trades_df.withColumn(
    "adjusted_price",
    col("price") * lit(1.05)  # 5% adjustment
)

# Complex expression
df = trades_df.withColumn(
    "price_ratio",
    when(col("volume") > 1000, col("price") / lit(100))
    .otherwise(lit(0))
)
```

## Best Practices and Tips

1. Chain operations efficiently:
```python
df_processed = trades_df \
    .filter(col("price") > 0) \
    .withColumn("value", col("price") * col("volume")) \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy("symbol") \
    .agg(sum("value").alias("total_value"))
```

2. Use appropriate window specifications:
```python
# For time-based calculations
time_window = Window \
    .partitionBy("symbol") \
    .orderBy("timestamp") \
    .rowsBetween(-9, 0)

# For running totals
cumulative_window = Window \
    .partitionBy("symbol") \
    .orderBy("timestamp") \
    .rowsBetween(Window.unboundedPreceding, 0)
```

3. Optimize performance:
```python
# Cache frequently used DataFrames
trades_df.cache()

# Repartition for better parallelism
trades_df = trades_df.repartition("symbol")

# Use appropriate number of partitions
spark.conf.set("spark.sql.shuffle.partitions", "200")
```

Remember to:
- Use appropriate data types for columns
- Handle null values appropriately
- Consider performance implications of window operations
- Monitor memory usage with large windows
- Use appropriate watermark delays for streaming
- Test with representative data volumes