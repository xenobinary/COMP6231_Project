import os
from google.cloud import bigquery
from google.cloud import firestore

def update_firestore_watchlist():
    # --- Configuration ---
    PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'comp6231-project') # Replace with your project ID
    DATASET_ID = 'stock'
    WATCHLIST_COLLECTION = 'watchlists'
    WATCHLIST_DOC_ID = 'adf_hurst_vr_screened'

    # Initialize Clients
    bq_client = bigquery.Client(project=PROJECT_ID)
    db = firestore.Client(project=PROJECT_ID)

    # --- 1. Run the BigQuery Query ---
    QUERY = """
    WITH base AS (
      SELECT symbol, date, close
      FROM `comp6231-project.stock.ohlcv_daily`
      WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) 
    ), aggregated AS (
      SELECT symbol, ARRAY_AGG(close ORDER BY date DESC LIMIT 200) AS price_array
      FROM base
      GROUP BY symbol
    ), stats AS (
      SELECT
        symbol,
        `comp6231-project.stock.hurst_exponent`(price_array) AS hurst,
        `comp6231-project.stock.variance_ratio`(price_array) AS vr
      FROM aggregated
    )
    SELECT symbol FROM stats
    WHERE hurst < 0.5 AND vr < 1.2
    ORDER BY hurst ASC
    LIMIT 50
    """
    
    query_job = bq_client.query(QUERY)
    results = query_job.result() # Waits for the job to complete

    # Extract the symbols into a list
    screened_symbols = [row.symbol for row in results]

    # --- 3. Write to Firestore ---
    doc_ref = db.collection(WATCHLIST_COLLECTION).document(WATCHLIST_DOC_ID)

    # Firestore update: Set a single document with the list of symbols
    # This overwrites the previous watchlist with the new results
    try:
        doc_ref.set({
            'symbols': screened_symbols,
            'last_updated': firestore.SERVER_TIMESTAMP
        })
        print(f"Successfully updated Firestore document '{WATCHLIST_DOC_ID}' with {len(screened_symbols)} symbols.")
        return screened_symbols
    except Exception as e:
        print(f"An error occurred updating Firestore: {e}")
        return None

if __name__ == "__main__":
    update_firestore_watchlist()
