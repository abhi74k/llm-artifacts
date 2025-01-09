# Market Data Processing with PySpark: A Comprehensive Guide

## Table of Contents
1. [Introduction](#introduction)
2. [Data Structures](#data-structures)
3. [Basic Processing](#basic-processing)
4. [Advanced Analytics](#advanced-analytics)
5. [State Management](#state-management)
6. [Performance Optimization](#performance-optimization)

## Introduction

This guide covers comprehensive market data processing using PySpark, including L2 order book data and trades processing, market microstructure calculations, and state management.

## Data Structures

### Basic Schema Definitions

```python
from pyspark.sql.types import *

# L2 Order Book Schema
l2_schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("seqnum", LongType(), False),
    StructField("timestamp", LongType(), False),
    StructField("levels", ArrayType(
        StructType([
            StructField("price", DoubleType(), False),
            StructField("quantity", DoubleType(), False),
            StructField("orders", IntegerType(), True)
        ])
    ), False)
])

# Trades Schema
trades_schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("seqnum", LongType(), False),
    StructField("timestamp", LongType(), False),
    StructField("price", DoubleType(), False),
    StructField("quantity", DoubleType(), False),
    StructField("side", StringType(), False)
])
```

## Basic Processing

### Loading and Interleaving Data

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

def load_market_data(spark, l2_path, trades_path):
    # Read both Parquet files
    l2_df = (spark.read.parquet(l2_path)
             .select(
                 col("seqnum"),
                 col("timestamp"),
                 col("symbol"),
                 col("levels"),
                 lit("L2").alias("record_type")
             ))

    trades_df = (spark.read.parquet(trades_path)
                .select(
                    col("seqnum"),
                    col("timestamp"),
                    col("symbol"),
                    col("price"),
                    col("quantity"),
                    col("side"),
                    lit("TRADE").alias("record_type")
                ))

    # Union and order by seqnum
    return (l2_df
            .unionByName(trades_df, allowMissingColumns=True)
            .orderBy("seqnum"))
```

## Advanced Analytics

### Market Microstructure Calculations

```python
from pyspark.sql import Window
from pyspark.sql import functions as F

def calculate_volatility_windows(df, windows=[60000, 300000]):
    """Calculate rolling volatility for different window sizes"""
    
    def add_volatility_window(df, window_ms):
        window_spec = Window.partitionBy("symbol")\
                           .orderBy("seqnum")\
                           .rangeBetween(-window_ms, 0)
        
        return (df
            .withColumn(f"returns_{window_ms}ms",
                F.when(F.col("record_type") == "TRADE",
                    F.log(F.col("price") / 
                         F.lag("price", 1).over(Window.partitionBy("symbol")
                                               .orderBy("seqnum"))))
                .otherwise(0))
            .withColumn(f"volatility_{window_ms}ms",
                F.sqrt(F.sum(F.pow(F.col(f"returns_{window_ms}ms"), 2))
                      .over(window_spec)) * 
                F.sqrt(F.lit(252 * 24 * 60 * 60 * 1000 / window_ms)))
        )
    
    result_df = df
    for window in windows:
        result_df = add_volatility_window(result_df, window)
    
    return result_df

def calculate_spreads(df):
    """Calculate various spread metrics"""
    return (df
        # Basic bid-ask spread
        .withColumn("quoted_spread",
            F.when(F.col("record_type") == "L2",
                  F.col("levels")[0].asks[0].price - 
                  F.col("levels")[0].bids[0].price)
            .otherwise(None))
        
        # Relative spread
        .withColumn("relative_spread",
            F.when(F.col("record_type") == "L2",
                  (F.col("levels")[0].asks[0].price - 
                   F.col("levels")[0].bids[0].price) /
                  ((F.col("levels")[0].asks[0].price + 
                    F.col("levels")[0].bids[0].price) / 2))
            .otherwise(None))
        
        # Depth-weighted spread
        .withColumn("depth_weighted_spread",
            F.when(F.col("record_type") == "L2",
                  F.aggregate(
                      F.slice(F.col("levels"), 1, 5),
                      F.lit(0),
                      lambda acc, level: acc + 
                          (level.asks.price - level.bids.price) * 
                          (level.asks.quantity + level.bids.quantity) / 2
                  ) / F.aggregate(
                      F.slice(F.col("levels"), 1, 5),
                      F.lit(0),
                      lambda acc, level: acc + 
                          (level.asks.quantity + level.bids.quantity) / 2
                  ))
            .otherwise(None))
    )
```

## State Management

### Using Classes for State Management

```python
from dataclasses import dataclass
from typing import Dict, List
import time

@dataclass
class MarketState:
    last_trade_price: float = 0.0
    rolling_volume: float = 0.0
    last_update_time: int = 0
    trade_count: int = 0
    price_levels: Dict[float, float] = None
    
    def __post_init__(self):
        if self.price_levels is None:
            self.price_levels = {}
        if self.last_update_time == 0:
            self.last_update_time = int(time.time() * 1000)

class MarketDataProcessor:
    def __init__(self, decay_time_ms: int = 300000):
        self.state = {}  # Dict[symbol, MarketState]
        self.decay_time_ms = decay_time_ms
    
    def process_trade(self, symbol: str, price: float, quantity: float, timestamp: int) -> Dict:
        if symbol not in self.state:
            self.state[symbol] = MarketState()
        
        state = self.state[symbol]
        
        # Decay old volume
        time_diff = timestamp - state.last_update_time
        if time_diff > 0:
            decay_factor = max(0, 1 - (time_diff / self.decay_time_ms))
            state.rolling_volume *= decay_factor
        
        # Update state
        state.last_trade_price = price
        state.rolling_volume += quantity
        state.trade_count += 1
        state.last_update_time = timestamp
        
        return {
            "symbol": symbol,
            "price": price,
            "rolling_volume": state.rolling_volume,
            "trade_count": state.trade_count
        }
```

### Using mapGroupsWithState for Streaming

```python
from pyspark.sql.streaming import GroupState

def update_state(key: str, inputs: Iterator[Row], state: GroupState) -> List[Row]:
    if not state.exists:
        state.update({
            "last_price": 0.0,
            "volume": 0.0,
            "vwap": 0.0,
            "last_update": 0
        })
    
    current_state = state.get
    outputs = []
    
    for row in inputs:
        # Decay old values
        time_diff = row.timestamp - current_state["last_update"]
        if time_diff > 0:
            decay_factor = max(0, 1 - (time_diff / 300000))
            current_state["volume"] *= decay_factor
        
        # Update state
        current_state["volume"] += row.quantity
        current_state["last_price"] = row.price
        current_state["vwap"] = ((current_state["vwap"] * current_state["volume"]) + 
                                (row.price * row.quantity)) / (current_state["volume"] + row.quantity)
        current_state["last_update"] = row.timestamp
        
        outputs.append(Row(
            symbol=key,
            timestamp=row.timestamp,
            price=row.price,
            rolling_volume=current_state["volume"],
            vwap=current_state["vwap"]
        ))
    
    state.update(current_state)
    return outputs
```

## Performance Optimization

### Key Optimization Techniques

1. Partition Management
```python
# Repartition by symbol for better parallelism
df = df.repartition("symbol")
```

2. Caching Strategies
```python
# Cache intermediate results
df.cache()
```

3. Window Function Optimization
```python
# Use rangeBetween instead of rowsBetween when possible
window_spec = Window.partitionBy("symbol")\
                   .orderBy("seqnum")\
                   .rangeBetween(-300000, 0)  # 5min window
```

4. Broadcast Join Optimization
```python
from pyspark.sql.functions import broadcast

# Use broadcast join for small reference data
result = large_df.join(broadcast(small_df), "symbol")
```

### Best Practices

1. Memory Management
- Monitor executor memory usage
- Use appropriate number of partitions
- Clear unnecessary caches

2. State Management
- Implement state cleanup mechanisms
- Use appropriate timeout values
- Monitor state size growth

3. Performance Monitoring
- Use Spark UI for monitoring
- Track processing times
- Monitor memory usage

## Conclusion

This guide covers the essential aspects of processing market data with PySpark. For optimal performance, always:
- Monitor your job's resource usage
- Tune partition sizes and window configurations
- Implement appropriate state cleanup mechanisms
- Use the right combination of declarative and imperative processing

For production deployments, consider:
- Adding error handling and logging
- Implementing monitoring and alerting
- Setting up proper cleanup mechanisms
- Testing with production-like data volumes
