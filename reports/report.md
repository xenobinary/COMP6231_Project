## Objective

This project aims to backtest (or realtime predict) the buy-in signal of the candidate stock symbols, which were screened by some statistical method (Hurst, VR test). We utilize a bunch of services in Google Serverless platform, including BigQuery, FireStore, CloudRun, Pub/Sub. We use a hybrid architecture since some programs run locally via API call. 

## Design 

1. Initialization
  - Fetch all the Nasdaq stock symbols' 2 years OHCLV (open, high, close, low prices and volume) with 1 day interval from Yahoo finance. 
  - Implemented by a local python script since this script is only for initialization and run once before the system runs continuously. 
  - Use Google `BigQuery` API write the data directly into an existing table `ohclv_1d` in Dataset `stocks` in Google BigQuery service.

2. Daily Batch
  - Fetch all the Nasdaq stock symbols' recent days (the latest day in table to now) OHCLV with 1 day interval from Yahoo finance.
  - Implemented by a CloudRun function using Python script.
  - Scheduled running in every midnight (the exact time depends on the yahoo finance, when the data is ready and avoid the maintenance window)
  - Append the data directly into `ohclv_1d` table

3. Screening
  - Schedule a `BigQuery` query script running weekly (Run once after initialization data is ready)
  - Use predefined User Defined Functions (UDFs) `(Hurst, VR test)` screening the stationary stock symbols.
  - Update the symbols into `Firestore` watchlist
  - Clear the symbols weekly

4. Producer
  - Fetch watched symbols' 5 min close price from yahoo finance
  - Implemented by a CloudRun function using Python script
  - Feed the price into Pub/Sub service

5. Monitor
  - Subscribe the price in Pub/Sub service
  - Keep all the daily data of watched symbols (from `Firestore` watchlist)
  - Check the buy-in criterion (`ADX > 25` and `Bollinger Band` cross lower line)
  - Signal if meets
  - Restart the program before the market open (5:00 am)
  - Implement as a local run program or web site (e.g., Python Flask/Django dashboard or Streamlit app)
 
6. Backtest Batch
  - Keep the daily data end with a specific date, such as `2025-11-10`
  - Fetch the 5 min data from the specific date till now, then write into `ohclv_5min` table in Dataset `stocks` in BigQuery.

7. Backtest Producer
  - Get the data from `ohclv_5min` table every 5 second
  - Then feed the data into Pub/Sub service

```mermaid
graph TB
    subgraph External["External Data Source"]
        YF[("Yahoo Finance API<br/>OHLCV Data")]
    end

    subgraph Init["1. Initialization (One-time)"]
        InitScript["Local Python Script"]
        InitData["Fetch 2 Years OHLCV<br/>1 Day Interval<br/>All NASDAQ Symbols"]
    end

    subgraph GCP["Google Cloud Platform"]
        subgraph Storage["Data Storage Layer"]
            BQ[("BigQuery<br/>Dataset: stocks")]
            BQTable1[("Table: ohlcv_1d<br/>Daily Data")]
            BQTable2[("Table: ohlcv_5min<br/>5-Min Data")]
            FS[("Firestore<br/>Watchlist")]
        end

        subgraph Batch["2. Daily Batch Process"]
            CloudRun1["Cloud Run Function<br/>Python"]
            Scheduler1["Cloud Scheduler<br/>Daily @ Midnight"]
            BatchFetch["Fetch Recent Days<br/>1 Day Interval<br/>Latest to Now"]
        end

        subgraph Screen["3. Screening Process"]
            Scheduler2["Cloud Scheduler<br/>Weekly"]
            BQQuery["BigQuery Query Script"]
            UDF["User Defined Functions<br/>• Hurst Exponent<br/>• VR Test"]
            Filter["Screen Stationary<br/>Stock Symbols"]
        end

        subgraph Prod["4. Producer (Real-time)"]
            CloudRun2["Cloud Run Function<br/>Python"]
            ProdFetch["Fetch 5-Min Close Price<br/>Watched Symbols"]
            PubSub[("Pub/Sub Service<br/>Price Stream")]
        end

        subgraph BackBatch["6. Backtest Batch"]
            BackBatchProc["Backtest Batch Process"]
            BackFetch["Fetch 5-Min Data<br/>From Specific Date<br/>e.g., 2025-11-10"]
        end

        subgraph BackProd["7. Backtest Producer"]
            BackProdProc["Backtest Producer<br/>Process"]
            BackTimer["Every 5 Seconds"]
        end
    end

    subgraph Local["Local Environment"]
        subgraph Monitor["5. Monitor Process"]
            MonApp["Flask/Django/Streamlit<br/>Dashboard"]
            SubProc["Pub/Sub Subscriber"]
            DataKeep["Keep Daily Data<br/>All Watched Symbols"]
            Criteria["Check Buy-in Criteria<br/>• ADX > 25<br/>• BB Cross Lower"]
            Signal["Generate Signal"]
            Restart["Auto Restart<br/>@ 5:00 AM"]
        end
    end

    %% Initialization Flow
    YF -->|"2 Years Historical"| InitScript
    InitScript --> InitData
    InitData -->|"Write"| BQTable1

    %% Daily Batch Flow
    Scheduler1 -.->|"Trigger Daily<br/>@ Midnight"| CloudRun1
    CloudRun1 --> BatchFetch
    YF -->|"Recent Days Data"| BatchFetch
    BatchFetch -->|"Append"| BQTable1

    %% Screening Flow
    Scheduler2 -.->|"Trigger Weekly"| BQQuery
    BQQuery --> UDF
    UDF --> Filter
    BQTable1 -->|"Query Daily Data"| UDF
    Filter -->|"Update/Clear Weekly"| FS

    %% Producer Flow
    FS -->|"Read Watchlist"| CloudRun2
    CloudRun2 --> ProdFetch
    YF -->|"5-Min Prices"| ProdFetch
    ProdFetch -->|"Publish"| PubSub

    %% Monitor Flow
    PubSub -->|"Subscribe"| SubProc
    SubProc --> MonApp
    FS -->|"Fetch Watchlist"| DataKeep
    BQTable1 -->|"Query Daily Data"| DataKeep
    DataKeep --> MonApp
    MonApp --> Criteria
    Criteria -->|"If Meets"| Signal
    Restart -.->|"Schedule"| MonApp

    %% Backtest Batch Flow
    BQTable1 -->|"Query Data<br/>Until Specific Date"| BackBatchProc
    YF -->|"5-Min Data<br/>From Date to Now"| BackFetch
    BackBatchProc --> BackFetch
    BackFetch -->|"Write"| BQTable2

    %% Backtest Producer Flow
    BackTimer -.->|"Trigger"| BackProdProc
    BQTable2 -->|"Read 5-Min Data"| BackProdProc
    BackProdProc -->|"Publish"| PubSub

    %% Styling
    classDef external fill:#e1f5ff,stroke:#01579b,stroke-width:2px
    classDef storage fill:#fff3e0,stroke:#e65100,stroke-width:2px
    classDef process fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    classDef scheduler fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
    classDef local fill:#fce4ec,stroke:#880e4f,stroke-width:2px

    class YF external
    class BQ,BQTable1,BQTable2,FS,PubSub storage
    class InitScript,CloudRun1,CloudRun2,BQQuery,BackBatchProc,BackProdProc process
    class Scheduler1,Scheduler2,BackTimer,Restart scheduler
    class MonApp local
```

