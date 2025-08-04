import pandas as pd
import logging

# Setup logger
logger = logging.getLogger("transactions_pipeline.transform")

# Transform Task Function
def transform_data(input_path, output_path) -> None:
    """
    Clean and aggregate transaction data, then save as new CSV.

    Args:
        input_path (str): Raw CSV path (from S3)
        output_path (str): Where to save the aggregated output for MySQL loading
    """
    try:
        logger.info(f"Reading raw data from {input_path}")
        df = pd.read_csv(input_path)

        # Clean
        logger.info("Cleaning data")
        df = df.dropna(subset=['amount', 'timestamp', 'bank_id'])
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df['bank_id'] = pd.to_numeric(df['bank_id'], errors='coerce')
        df = df.dropna(subset=['amount', 'timestamp', 'bank_id'])

        # Extract date
        df['date'] = df['timestamp'].dt.date
        logger.info(f"Extracted dates. Row count after cleaning: {len(df)}")

        # Aggregate
        logger.info("Aggregating data")
        result = df.groupby(['bank_id', 'date'], as_index=False)['amount'].sum()
        result.rename(columns={'amount': 'total_transaction_volume'}, inplace=True)

        # Save to CSV
        result.to_csv(output_path, index=False)
        logger.info(f"Transformed data saved to {output_path}")

    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise

    # End of Transform 
    logger.info("Data transformation completed successfully.")
