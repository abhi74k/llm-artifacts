# Building Market Data Signals with PySpark: A Comprehensive Guide

## Table of Contents
1. [PySpark Fundamentals](#fundamentals)
2. [Working with Market Data](#market-data)
3. [Feature Engineering for Market Data](#feature-engineering)
4. [State Management in PySpark](#state-management)
5. [Model Building for Next Tick Prediction](#model-building)

## PySpark Fundamentals <a name="fundamentals"></a>

### Setting Up PySpark
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

# Create Spark session with market data optimization configs
spark = SparkSession.builder \
    .appName("MarketDataAnalysis") \
    .config("spark.sql.files.maxPartitionBytes", "128m") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.default.parallelism", "100") \
    .getOrCreate()
```

### Basic DataFrame Operations
```python
# Reading Parquet files with partitioning
df = spark.read.parquet("s3://market-data/date=*/symbol=*/data")

# Basic transformations
df_cleaned = df \
    .filter(F.col("price") > 0) \
    .withColumn("timestamp", F.from_unixtime("epoch_time")) \
    .withColumn("log_price", F.log("price"))

# Window functions for time-based calculations
window_spec = Window.partitionBy("symbol") \
    .orderBy("timestamp") \
    .rangeBetween(-300, 0)  # 5-minute window

df_with_features = df_cleaned \
    .withColumn("avg_price_5min", F.avg("price").over(window_spec)) \
    .withColumn("vol_5min", F.stddev("price").over(window_spec))
```

## Working with Market Data <a name="market-data"></a>

### Loading Data for Multiple Symbols and Dates
```python
def load_market_data(start_date, end_date, symbols=None):
    """
    Load market data for specified date range and symbols
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        symbols (list): List of symbols to load. If None, loads all symbols
    """
    base_path = "s3://market-data/"
    
    # Generate date filters
    date_filter = F.col("date").between(start_date, end_date)
    
    # Build symbol filter if specified
    symbol_filter = F.col("symbol").isin(symbols) if symbols else F.lit(True)
    
    df = spark.read.parquet(base_path) \
        .filter(date_filter & symbol_filter)
    
    return df

# Example usage
df = load_market_data(
    start_date="2024-01-01",
    end_date="2024-01-31",
    symbols=["AAPL", "GOOGL", "MSFT"]
)
```

### Data Quality Checks
```python
def validate_market_data(df):
    """
    Perform data quality checks on market data
    """
    # Check for missing values
    null_counts = df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ])
    
    # Check for price anomalies
    price_stats = df.select(
        F.min("price").alias("min_price"),
        F.max("price").alias("max_price"),
        F.avg("price").alias("avg_price"),
        F.stddev("price").alias("stddev_price")
    )
    
    # Check for timestamp continuity
    window_spec = Window.partitionBy("symbol").orderBy("timestamp")
    time_gaps = df \
        .withColumn("prev_timestamp", F.lag("timestamp").over(window_spec)) \
        .withColumn("time_gap", F.col("timestamp") - F.col("prev_timestamp")) \
        .filter(F.col("time_gap") > F.lit(60))  # Gaps longer than 1 minute
    
    return null_counts, price_stats, time_gaps
```

## Feature Engineering for Market Data <a name="feature-engineering"></a>

### Building Order Book Features
```python
def create_orderbook_features(df):
    """
    Create features from order book data
    """
    # Define windows for different time horizons
    windows = {
        "1min": 60,
        "5min": 300,
        "15min": 900
    }
    
    # Base features
    df = df.withColumns({
        "mid_price": (F.col("bid_price1") + F.col("ask_price1")) / 2,
        "spread": F.col("ask_price1") - F.col("bid_price1"),
        "bid_depth": F.col("bid_size1") + F.col("bid_size2") + F.col("bid_size3"),
        "ask_depth": F.col("ask_size1") + F.col("ask_size2") + F.col("ask_size3"),
        "imbalance": (F.col("bid_size1") - F.col("ask_size1")) / 
                     (F.col("bid_size1") + F.col("ask_size1"))
    })
    
    # Create time-based features for each window
    for window_name, seconds in windows.items():
        window_spec = Window.partitionBy("symbol") \
            .orderBy("timestamp") \
            .rangeBetween(-seconds, 0)
        
        # Price features
        df = df.withColumns({
            f"mid_price_mean_{window_name}": F.avg("mid_price").over(window_spec),
            f"mid_price_std_{window_name}": F.stddev("mid_price").over(window_spec),
            f"spread_mean_{window_name}": F.avg("spread").over(window_spec),
            
            # Volume features
            f"volume_{window_name}": F.sum("volume").over(window_spec),
            f"trades_{window_name}": F.count("*").over(window_spec),
            
            # Order book features
            f"bid_depth_mean_{window_name}": F.avg("bid_depth").over(window_spec),
            f"ask_depth_mean_{window_name}": F.avg("ask_depth").over(window_spec),
            f"imbalance_mean_{window_name}": F.avg("imbalance").over(window_spec)
        })
    
    return df
```

### Creating Complex Signals with State

```python
from pyspark.sql import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
import numpy as np

# Define state management class
class SignalState:
    def __init__(self):
        self.ema_alpha = 0.1
        self.last_ema = None
        self.vol_lookback = []
        self.vol_window = 20
    
    def update_ema(self, price):
        if self.last_ema is None:
            self.last_ema = price
        else:
            self.last_ema = (1 - self.ema_alpha) * self.last_ema + \
                           self.ema_alpha * price
        return self.last_ema
    
    def update_volatility(self, price):
        self.vol_lookback.append(price)
        if len(self.vol_lookback) > self.vol_window:
            self.vol_lookback.pop(0)
        if len(self.vol_lookback) < 2:
            return 0.0
        return np.std(self.vol_lookback)

# Create UDFs for stateful computations
@udf(DoubleType())
def compute_adaptive_ema(prices, alphas):
    state = SignalState()
    results = []
    for price, alpha in zip(prices, alphas):
        state.ema_alpha = alpha
        results.append(state.update_ema(price))
    return results[-1]

def create_stateful_signals(df):
    """
    Create signals that maintain state across rows
    """
    # Define windows for state management
    symbol_window = Window.partitionBy("symbol").orderBy("timestamp")
    
    # Collect price arrays for stateful computation
    df_with_arrays = df \
        .withColumn("price_array", 
                   F.collect_list("price").over(symbol_window)) \
        .withColumn("alpha_array",
                   F.collect_list("adaptive_alpha").over(symbol_window))
    
    # Apply stateful computations
    df_signals = df_with_arrays \
        .withColumn("adaptive_ema", 
                   compute_adaptive_ema("price_array", "alpha_array"))
    
    return df_signals
```

## Model Building for Next Tick Prediction <a name="model-building"></a>

### Data Preparation
```python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

def prepare_training_data(df, feature_cols, target_col="next_tick_return"):
    """
    Prepare data for model training
    """
    # Create target variable (next tick return)
    window_spec = Window.partitionBy("symbol").orderBy("timestamp")
    
    df_ml = df \
        .withColumn("next_price", 
                   F.lead("mid_price").over(window_spec)) \
        .withColumn(target_col,
                   (F.col("next_price") - F.col("mid_price")) / 
                   F.col("mid_price")) \
        .dropna()
    
    # Assemble features
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features"
    )
    
    return assembler.transform(df_ml)

### Model Training
def train_next_tick_model(df_train, df_val):
    """
    Train model for next tick prediction
    """
    # Initialize model
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol="next_tick_return",
        maxIter=100,
        maxDepth=5
    )
    
    # Train model
    model = gbt.fit(df_train)
    
    # Evaluate model
    predictions = model.transform(df_val)
    evaluator = RegressionEvaluator(
        labelCol="next_tick_return",
        predictionCol="prediction",
        metricName="rmse"
    )
    
    rmse = evaluator.evaluate(predictions)
    
    return model, rmse

# Example usage
feature_cols = [
    "mid_price_mean_1min",
    "mid_price_std_1min",
    "spread_mean_1min",
    "volume_1min",
    "imbalance_mean_1min",
    "adaptive_ema"
]

# Split data
df_train, df_val = df.randomSplit([0.8, 0.2], seed=42)

# Prepare data
df_train_prepared = prepare_training_data(df_train, feature_cols)
df_val_prepared = prepare_training_data(df_val, feature_cols)

# Train and evaluate model
model, rmse = train_next_tick_model(df_train_prepared, df_val_prepared)
```

### Production Deployment

```python
def create_production_pipeline(spark, model_path):
    """
    Create production pipeline for real-time prediction
    """
    def process_tick(df_tick):
        # Create features
        df_features = create_orderbook_features(df_tick)
        df_features = create_stateful_signals(df_features)
        
        # Prepare for prediction
        df_prepared = prepare_training_data(
            df_features,
            feature_cols,
            target_col=None
        )
        
        # Load model and make predictions
        model = GBTRegressor.load(model_path)
        predictions = model.transform(df_prepared)
        
        return predictions
    
    return process_tick

# Example streaming usage
from pyspark.sql.streaming import StreamingQuery

def start_prediction_stream(spark, input_path, output_path):
    """
    Start streaming predictions
    """
    # Create processing pipeline
    pipeline = create_production_pipeline(spark, "s3://models/next_tick_model")
    
    # Create streaming query
    query = spark.readStream \
        .format("parquet") \
        .load(input_path) \
        .transform(pipeline) \
        .writeStream \
        .outputMode("append") \
        .option("checkpointLocation", "s3://checkpoints/next_tick") \
        .start(output_path)
    
    return query
```

## Best Practices and Optimization Tips

1. **Data Partitioning**
   - Partition data by date and symbol for efficient querying
   - Use appropriate partition sizes (typically 100MB-1GB)
   - Avoid over-partitioning which can lead to small file problems

2. **Memory Management**
   - Cache frequently used DataFrames
   - Use broadcast joins for small reference data
   - Monitor memory usage with Spark UI

3. **Performance Optimization**
   - Use appropriate number of partitions
   - Optimize window operations by limiting window size
   - Use appropriate data types (e.g., TimestampType for timestamps)

4. **Production Considerations**
   - Implement proper error handling
   - Add monitoring and logging
   - Regular model retraining
   - Implement circuit breakers for production systems

## Conclusion

This guide covers the fundamentals of building market data signals in PySpark, from basic setup to complex feature engineering and model deployment. Remember to always test thoroughly and monitor performance in production environments.

For next steps, consider:
- Implementing more sophisticated features
- Adding cross-validation for model training
- Implementing online learning capabilities
- Adding monitoring and alerting systems
- Optimizing for specific market conditions