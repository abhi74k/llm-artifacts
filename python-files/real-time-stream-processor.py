"""
Real-time Market Data Stream Processor using PySpark
Processes market data from parquet files and outputs processed data in parquet format.

Usage Example:
    # Create processor with custom paths
    processor = StreamProcessor(
        input_path="path/to/input/parquet",
        output_path="path/to/output/parquet"
    )

    # Start processing with existing parquet files
    query = processor.process_stream(use_test_data=False)

    # Or generate and process test data
    query = processor.process_stream(use_test_data=True)

Output Structure:
    output/market_data/
        symbol=AAPL/
            part-00000-xxx.parquet
            part-00001-xxx.parquet
        symbol=GOOGL/
            part-00000-xxx.parquet
        checkpoint/
            # Checkpoint files

The output is partitioned by symbol for efficient querying of specific symbols.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.streaming import GroupState
from pyspark.sql.functions import *
from typing import Dict, Any
from datetime import datetime
import json
import os

# Define schema for market data updates
MARKET_DATA_SCHEMA = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", DoubleType(), True)
])

class MarketDataState:
    """State manager for market data processing"""
    
    def __init__(self, window_size_seconds: int = 300):
        self.window_size = window_size_seconds
        self.prices = []
        self.volumes = []
        self.timestamps = []
        self.last_update = None
        self.moving_average = None
        self.vwap = None
        self.total_volume = 0
        
    def update_with_tick(self, 
                        price: float, 
                        volume: float, 
                        timestamp: float) -> Dict[str, Any]:
        """
        Update state with new market data tick
        
        Args:
            price: Current tick price
            volume: Current tick volume
            timestamp: Tick timestamp in seconds since epoch
            
        Returns:
            Dict containing current metrics
        """
        # Add new tick
        self.prices.append(price)
        self.volumes.append(volume)
        self.timestamps.append(timestamp)
        self.last_update = timestamp
        self.total_volume += volume
        
        # Remove expired data points
        cutoff_time = timestamp - self.window_size
        while self.timestamps and self.timestamps[0] < cutoff_time:
            self.total_volume -= self.volumes[0]
            self.prices.pop(0)
            self.volumes.pop(0)
            self.timestamps.pop(0)
        
        # Recalculate metrics
        self._calculate_metrics()
        
        return self._get_current_metrics()
    
    def _calculate_metrics(self):
        """Calculate moving average and VWAP"""
        if self.prices:
            self.moving_average = sum(self.prices) / len(self.prices)
            if self.total_volume > 0:
                self.vwap = sum(p * v for p, v in zip(self.prices, self.volumes)) / self.total_volume
    
    def _get_current_metrics(self) -> Dict[str, Any]:
        """Get current state of all metrics"""
        return {
            "moving_average": self.moving_average,
            "vwap": self.vwap,
            "num_ticks": len(self.prices),
            "total_volume": self.total_volume,
            "last_update": self.last_update
        }

def process_market_tick(key: str, row: Row, state: GroupState) -> Dict[str, Any]:
    """
    Process each individual market data tick
    
    Args:
        key: Symbol being processed
        row: Current market data tick
        state: GroupState maintaining symbol state
        
    Returns:
        Dict containing current metrics for the symbol
    """
    # Initialize or get state
    if not state.exists:
        state.update(MarketDataState())
    
    market_state = state.get
    
    # Process this tick
    metrics = market_state.update_with_tick(
        price=row.price,
        volume=row.volume,
        timestamp=row.timestamp.timestamp()
    )
    
    # Update state
    state.update(market_state)
    
    # Return current metrics
    return {
        "symbol": key,
        "timestamp": row.timestamp,
        "moving_average": metrics["moving_average"],
        "vwap": metrics["vwap"],
        "num_ticks": metrics["num_ticks"],
        "total_volume": metrics["total_volume"]
    }

class StreamProcessor:
    """Main stream processing class"""
    
    def __init__(self, 
                 app_name: str = "RealTimeMarketData",
                 input_path: str = "input/market_data",
                 output_path: str = "output/market_data"):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.streaming.schemaInference", "true") \
            .getOrCreate()
        self.input_path = input_path
        self.output_path = output_path
        
        # Ensure output directory exists
        os.makedirs(output_path, exist_ok=True)
        os.makedirs(f"{output_path}/checkpoint", exist_ok=True)
    
    def setup_parquet_stream(self):
        """Setup streaming source from parquet files"""
        return self.spark.readStream \
            .format("parquet") \
            .schema(MARKET_DATA_SCHEMA) \
            .option("maxFilesPerTrigger", 1) \  # Process one file at a time
            .option("cleanSource", "archive") \  # Archive processed files
            .option("sourceArchiveDir", f"{self.input_path}/processed") \
            .load(self.input_path)
    
    def setup_test_stream(self, rows_per_second: int = 1):
        """Setup test data stream and write to parquet"""
        # Create test data
        test_df = self.spark.createDataFrame(
            self.spark.sparkContext.parallelize([]),
            MARKET_DATA_SCHEMA
        )
        
        # Write test data as parquet
        def write_test_data(batch_id):
            timestamp = datetime.now()
            test_data = [(
                timestamp,
                "AAPL",
                100 + float(i),
                float(100 * (i % 10))
            ) for i in range(rows_per_second)]
            
            batch_df = self.spark.createDataFrame(test_data, MARKET_DATA_SCHEMA)
            batch_df.write.parquet(
                f"{self.input_path}/batch_{timestamp.timestamp()}.parquet",
                mode="append"
            )
        
        return self.spark.readStream \
            .format("rate") \
            .option("rowsPerSecond", rows_per_second) \
            .load() \
            .select(
                current_timestamp().alias("timestamp"),
                lit("AAPL").alias("symbol"),
                (rand() * 100 + 100).alias("price"),
                (rand() * 100).cast("integer").alias("volume")
            ) \
            .writeStream \
            .foreachBatch(write_test_data) \
            .start()
    
    def process_stream(self, use_test_data: bool = False):
        """
        Start stream processing
        
        Args:
            use_test_data: If True, generate test data, else use existing parquet files
        """
        # Setup test data if requested
        if use_test_data:
            self.setup_test_stream()
        
        # Setup input stream
        stream_df = self.setup_parquet_stream()
        
        # Process each tick with state
        result_df = stream_df \
            .groupByKey(lambda x: x.symbol) \
            .flatMapGroupsWithState(
                process_market_tick,
                outputMode="update",
                timeoutConf="NoTimeout"
            )
        
        # Write output to parquet
        query = result_df.writeStream \
            .outputMode("append") \  # Use append mode for parquet
            .format("parquet") \
            .option("path", self.output_path) \
            .option("checkpointLocation", f"{self.output_path}/checkpoint") \
            .partitionBy("symbol") \  # Partition output by symbol
            .trigger(processingTime="1 second") \
            .start()
        
        return query

def main():
    # Create processor instance with parquet paths
    processor = StreamProcessor(
        input_path="input/market_data",
        output_path="output/market_data"
    )
    
    # Start processing
    query = processor.process_stream(use_test_data=True)  # Use test data by default
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Shutting down stream processor...")
        query.stop()
        
    # Print output location
    print(f"Processed data written to: {processor.output_path}")
    print("Output is partitioned by symbol for efficient querying")

if __name__ == "__main__":
    main()
