# Introduction


# Background

In our project, we build our solution entirely on Google Cloud Platform (GCP). We apply the Serverless architecture and use several core services provided by GCP, including BigQuery, Cloud Run, Pub/Sub, and Firestore. Although we do not utilize all the functionalities of these services, we practice how to tailor these services to real scenarios for learning purposes. The following sections present the technical specifications of GCP's serverless architecture and each service we employ. Besides the overall view, we focus on the distributed aspects aligning with our Distributed System course.

## Google Cloud Platform

Google Cloud Platform (GCP) is a suite of cloud computing services offered by Google that provides a series of modular cloud services including computing, data storage, data analytics, and machine learning, alongside a set of management tools \cite{google_cloud_products}. GCP provides infrastructure as a service, platform as a service, and serverless computing environments across multiple geographic regions worldwide.

From a distributed systems perspective, GCP implements fundamental principles including data replication, fault tolerance, and horizontal scalability across its global infrastructure \cite{barroso2013datacenter}. The platform abstracts away much of the complexity of building distributed systems by providing managed services that handle concerns such as load balancing, automatic scaling, and data consistency \cite{burns2016borg}.

### Serverless Computing

Serverless computing represents a cloud execution model where the cloud provider dynamically manages the allocation and provisioning of servers \cite{jonas2019cloud}. In this paradigm, developers focus on writing application logic without concerning themselves with infrastructure management, server provisioning, or capacity planning. Despite its name, serverless computing still runs on servers, but these servers are abstracted away from the application development process \cite{castro2019serverless}.

From a distributed systems viewpoint, serverless platforms like GCP's Cloud Run and Cloud Functions implement several key distributed computing principles. **Auto-scaling** is achieved through dynamic resource allocation where the platform automatically spawns new container instances in response to incoming request loads and terminates idle instances to optimize resource utilization \cite{schleier2021serverless}. **Fault tolerance** is built-in through automatic request retry mechanisms and instance replacement when failures occur \cite{wang2018peeking}. **Geographic distribution** enables low-latency access by deploying functions across multiple regions, bringing computation closer to end users \cite{jonas2019cloud}.

The event-driven nature of serverless computing aligns well with distributed system architectures. Services communicate asynchronously through event triggers and message queues, promoting loose coupling between components \cite{baldini2017serverless}. This architecture enhances system resilience since individual component failures do not cascade throughout the system. However, serverless computing also introduces distributed systems challenges including cold start latency when instantiating new function instances, limited execution duration constraints, and statelessness requiring external storage for maintaining application state \cite{schleier2021serverless}.

### BigQuery

BigQuery is Google's fully managed, serverless data warehouse solution designed for large-scale data analytics \cite{melnik2020dremel}. It enables super-fast SQL queries using the processing power of Google's infrastructure and can scale seamlessly to petabytes of data. BigQuery separates storage and compute resources, allowing independent scaling of each component based on workload requirements \cite{melnik2020dremel}.

The distributed architecture of BigQuery is built on several foundational technologies. At its core, BigQuery utilizes **Dremel**, Google's distributed query engine that enables interactive analysis of nested data structures \cite{melnik2010dremel}. Dremel employs a tree-based serving architecture where queries are decomposed into smaller sub-queries distributed across thousands of nodes for parallel execution. Each node processes a portion of the data, and results are aggregated back up the tree hierarchy \cite{melnik2010dremel}.

For storage, BigQuery leverages **Colossus**, Google's distributed file system that provides durability and availability through data replication across multiple data centers \cite{corbett2013spanner}. Data is stored in a columnar format optimized for analytical workloads, enabling efficient compression and minimizing I/O operations for queries that access specific columns \cite{melnik2020dremel}. 

BigQuery implements **strong consistency** guarantees for its storage layer, ensuring that reads always reflect the most recent writes \cite{google_bigquery_consistency}. This is particularly important for algorithmic trading applications where stale data could lead to incorrect trading decisions. The system achieves high availability through multi-region replication with automatic failover capabilities \cite{google_bigquery_docs}.

**Query optimization** in BigQuery occurs through a distributed query planner that analyzes query patterns, estimates data sizes, and determines optimal execution strategies including join ordering and partition pruning \cite{melnik2020dremel}. The system dynamically allocates compute resources based on query complexity, automatically parallelizing operations across available slots (units of computational capacity).

### Cloud Run

Cloud Run is a fully managed compute platform that automatically scales stateless containers \cite{google_cloudrun_docs}. It abstracts away infrastructure management and allows developers to deploy containerized applications that respond to HTTP requests or events. Cloud Run builds upon Knative, an open-source Kubernetes-based platform for deploying serverless workloads \cite{knative_docs}.

From a distributed systems perspective, Cloud Run implements several critical mechanisms. **Container orchestration** is managed through Kubernetes primitives adapted for serverless execution \cite{burns2018kubernetes}. When a request arrives, Cloud Run automatically instantiates container instances, routes traffic to healthy instances, and scales down to zero when idle. This dynamic scaling is achieved through a control plane that monitors request queues and system metrics to make provisioning decisions \cite{knative_docs}.

**Load balancing** in Cloud Run distributes incoming requests across multiple container instances using Google's global load balancer infrastructure \cite{google_cloudrun_docs}. The load balancer performs health checks on instances and removes unhealthy containers from the serving pool, implementing fault tolerance through automatic replacement. Traffic splitting capabilities enable gradual rollouts and A/B testing by directing specified percentages of requests to different service revisions \cite{google_cloudrun_docs}.

**Request routing** follows a path through multiple distributed components including global anycast IPs that route users to the nearest Google point of presence, regional load balancers that distribute requests within a region, and service meshes that handle instance-level routing \cite{google_cloudrun_architecture}. This multi-tier architecture reduces latency and improves reliability through geographic distribution.

Cloud Run provides **concurrency control** allowing each container instance to handle multiple simultaneous requests up to a configurable limit \cite{google_cloudrun_docs}. This contrasts with function-as-a-service platforms where each invocation typically handles a single request. The concurrency model must be carefully tuned for algorithmic trading applications to balance throughput against processing latency for time-sensitive operations.

### Pub/Sub

Google Cloud Pub/Sub is a fully managed, real-time messaging service that enables asynchronous communication between independent applications \cite{google_pubsub_docs}. It implements the publish-subscribe pattern where publishers send messages to topics without knowledge of subscribers, and subscribers receive messages from topics without knowledge of publishers. This decoupling is a fundamental principle in distributed systems design \cite{eugster2003many}.

The distributed architecture of Pub/Sub provides **at-least-once delivery** guarantees, ensuring that messages are delivered to subscribers even in the presence of failures \cite{google_pubsub_docs}. This is achieved through message persistence and acknowledgment protocols. When a subscriber receives a message, it must acknowledge receipt within a configurable deadline. If acknowledgment is not received, Pub/Sub automatically redelivers the message \cite{google_pubsub_docs}.

**Horizontal scalability** is inherent in Pub/Sub's design. The service automatically partitions message flow across multiple distributed servers, allowing it to handle millions of messages per second \cite{google_pubsub_scalability}. Publishers and subscribers can scale independently, and the system dynamically allocates resources based on message throughput and subscriber count.

**Message ordering** in Pub/Sub requires special consideration. By default, messages are delivered in no particular order to maximize throughput and availability \cite{google_pubsub_docs}. However, Pub/Sub supports ordering keys that guarantee messages with the same key are delivered to subscribers in the order they were published \cite{google_pubsub_ordering}. This is implemented through affinity routing where messages with identical ordering keys are processed by the same distributed worker.

For algorithmic trading systems, Pub/Sub serves as the **event backbone** connecting various distributed components. Market data feeds can publish price updates to topics, triggering downstream processing in Cloud Run containers and BigQuery streaming inserts. The asynchronous nature prevents blocking operations and enables parallel processing of multiple trading signals \cite{fowler2002patterns}.

**Geo-replication** capabilities allow Pub/Sub topics to replicate messages across multiple regions, improving availability and reducing latency for globally distributed subscribers \cite{google_pubsub_docs}. This multi-region architecture provides disaster recovery capabilities and enables compliance with data residency requirements.

### Firestore

Firestore is a NoSQL document database built for automatic scaling, high performance, and ease of application development \cite{google_firestore_docs}. As a distributed database, Firestore provides strong consistency guarantees, ACID transactions, and automatic multi-region replication \cite{google_firestore_docs}.

The distributed architecture of Firestore is built on Google's **Spanner** technology, which pioneered the use of TrueTime for globally consistent transactions \cite{corbett2013spanner}. TrueTime is a distributed time API that leverages GPS and atomic clocks to provide globally synchronized timestamps with bounded uncertainty \cite{corbett2013spanner}. This enables Firestore to offer external consistency, meaning that if a transaction T1 commits before transaction T2 begins, then T1's timestamp is smaller than T2's timestamp \cite{corbett2013spanner}.

**Data replication** in Firestore occurs across multiple zones within a region for standard databases, or across multiple regions for multi-region configurations \cite{google_firestore_docs}. Replication follows a consensus protocol similar to Paxos, where a quorum of replicas must acknowledge writes before they are committed \cite{lamport2001paxos}. This ensures durability and availability even when some replicas fail.

**Query processing** in Firestore is optimized for document-oriented data models. The database automatically creates indexes to support efficient queries, and queries are executed distributedly across shards based on collection size and access patterns \cite{google_firestore_docs}. For algorithmic trading applications, Firestore can store trading signals, portfolio positions, and execution logs with sub-millisecond read latency.

**Real-time listeners** enable clients to subscribe to document or query snapshots and receive immediate notifications when data changes \cite{google_firestore_docs}. This is implemented through a distributed streaming infrastructure that maintains long-lived connections and pushes updates from the database to connected clients. This feature is particularly valuable for trading dashboards that require real-time updates on position changes and market conditions.

**Transactions** in Firestore support both optimistic concurrency control through versioning and pessimistic locking mechanisms \cite{google_firestore_transactions}. Optimistic transactions are suitable for low-contention scenarios where conflicts are rare, while pessimistic locks prevent concurrent modifications to critical trading data such as account balances and position limits.

### CLI and Cloud API

The Google Cloud Command-Line Interface (CLI) and Cloud APIs provide programmatic access to GCP services, enabling infrastructure as code and automated deployments \cite{google_cloud_cli}. The Cloud SDK includes command-line tools for managing resources, while REST and gRPC APIs enable application integration \cite{google_cloud_apis}.

From a distributed systems perspective, these interfaces implement **idempotent operations** where repeated requests with the same parameters produce the same result \cite{google_cloud_apis}. This is critical for building reliable distributed applications since network failures may cause clients to retry requests without knowing if previous attempts succeeded.

**Authentication and authorization** follow OAuth 2.0 standards with service accounts enabling machine-to-machine communication \cite{google_cloud_iam}. Credentials can be scoped to specific API permissions following the principle of least privilege. For distributed trading systems, different components can authenticate with minimal required permissions, limiting potential damage from compromised credentials.

The Cloud APIs implement **rate limiting** and **quota management** to ensure fair resource allocation across users and prevent system overload \cite{google_cloud_quotas}. Applications must implement exponential backoff and retry logic to handle quota exceeded errors gracefully. This distributed rate limiting is enforced through distributed counters maintained across Google's infrastructure.

**API versioning** strategies enable backward compatibility as services evolve \cite{google_cloud_apis}. Multiple API versions can coexist, allowing clients to migrate gradually. For long-running trading systems, this versioning prevents breaking changes from disrupting operations.

## Algorithm Trading

Algorithmic trading uses computer programs to execute trading strategies based on predefined rules and market conditions \cite{aldridge2013high}. These systems analyze market data, generate trading signals, and execute orders automatically with minimal human intervention. The field has grown significantly with advances in computing power and data availability \cite{kissell2013science}.

### Mean Reversion Strategy

Mean reversion is a trading strategy based on the theory that asset prices and returns eventually move back toward their historical mean or average \cite{pole2007statistical}. The strategy assumes that extreme price movements are temporary and prices will revert to a long-term equilibrium level \cite{chan2009quantitative}.

From a statistical perspective, mean reversion implies that asset returns exhibit negative serial correlation \cite{poterba1988mean}. When prices deviate significantly from their mean, traders take positions expecting the price to return toward the average. Long positions are established when prices fall below the mean, and short positions when prices rise above \cite{chan2009quantitative}.

The effectiveness of mean reversion strategies depends on identifying assets that exhibit mean-reverting behavior and determining appropriate entry and exit thresholds \cite{pole2007statistical}. Statistical tests such as the Augmented Dickey-Fuller test and Hurst exponent analysis help identify mean-reverting securities \cite{tsay2005analysis}.

### Statistical Screening

Statistical screening methods evaluate whether financial time series exhibit mean-reverting characteristics suitable for algorithmic trading strategies \cite{chan2009quantitative}. These techniques analyze historical price data to identify securities with predictable patterns that can be exploited for profit.

#### Hurst Exponent

The Hurst exponent (H) quantifies the long-term memory and persistence of a time series \cite{hurst1951long}. It measures the tendency of a time series to either regress to its mean or cluster in a particular direction \cite{mandelbrot1968fractional}. The Hurst exponent ranges from 0 to 1, where values provide different interpretations of time series behavior.

When H = 0.5, the time series exhibits random walk behavior similar to geometric Brownian motion, indicating no predictable patterns \cite{mandelbrot1968fractional}. Values of H < 0.5 suggest mean-reverting behavior where increases are likely followed by decreases and vice versa \cite{chan2009quantitative}. This anti-persistent behavior is desirable for mean reversion trading strategies. Conversely, H > 0.5 indicates trending or persistent behavior where price movements tend to continue in the same direction \cite{mandelbrot1968fractional}.

Several methods exist for estimating the Hurst exponent including rescaled range analysis, detrended fluctuation analysis, and periodogram regression \cite{kantelhardt2002multifractal}. For algorithmic trading, the rescaled range (R/S) method proposed by Hurst is commonly used \cite{hurst1951long}. The R/S statistic scales with time according to a power law, and the Hurst exponent is estimated from the slope of the log-log plot of R/S versus time lag \cite{mandelbrot1968fractional}.

#### Variance Ratio Test

The variance ratio test evaluates the random walk hypothesis by comparing variances at different time horizons \cite{lo1988stock}. Under a random walk, the variance of returns should scale linearly with the time interval, meaning the variance of k-period returns equals k times the variance of one-period returns \cite{lo1988stock}.

The variance ratio VR(k) for a k-period holding interval is defined as the ratio of 1/k times the variance of k-period returns to the variance of one-period returns \cite{lo1988stock}. If the time series follows a random walk, VR(k) should equal 1 for all k. Values of VR(k) less than 1 indicate mean reversion, as the variance grows slower than linearly with the time horizon \cite{poterba1988mean}. Values greater than 1 suggest momentum or trending behavior \cite{lo1988stock}.

Lo and MacKinlay developed a heteroskedasticity-consistent variance ratio test that accounts for time-varying volatility common in financial data \cite{lo1988stock}. The test statistic follows a standard normal distribution under the null hypothesis of random walk, enabling statistical inference about mean reversion \cite{lo1988stock}. For algorithmic trading, securities with variance ratios significantly less than 1 at multiple time horizons are candidates for mean reversion strategies.

### Technical Indicators

Technical indicators are mathematical calculations based on price, volume, or open interest that traders use to forecast future price movements \cite{murphy1999technical}. These indicators help identify trading opportunities by analyzing market trends, momentum, and volatility \cite{pring2002technical}.

#### Average Directional Index (ADX)

The Average Directional Index (ADX) measures the strength of a trend regardless of its direction \cite{wilder1978new}. Developed by J. Welles Wilder Jr., the ADX is derived from the Directional Movement System which also produces two other indicators: the Positive Directional Indicator (+DI) and Negative Directional Indicator (-DI) \cite{wilder1978new}.

The calculation begins with determining directional movement by comparing consecutive price highs and lows \cite{wilder1978new}. Positive directional movement (+DM) occurs when the current high exceeds the previous high by more than the difference between the current low and previous low. Negative directional movement (-DM) occurs in the opposite scenario. The True Range (TR) measures volatility as the greatest of: current high minus current low, absolute value of current high minus previous close, or absolute value of current low minus previous close \cite{wilder1978new}.

Directional indicators are calculated by dividing smoothed averages of +DM and -DM by smoothed TR, then multiplying by 100 to produce +DI and -DI \cite{wilder1978new}. The Directional Movement Index (DX) is computed as 100 times the absolute difference between +DI and -DI divided by their sum. Finally, ADX is a moving average (typically 14-period) of DX values \cite{wilder1978new}.

ADX values range from 0 to 100. Values below 20 indicate weak or absent trends, suggesting sideways or choppy markets suitable for mean reversion strategies \cite{murphy1999technical}. Values above 25 indicate strengthening trends, while values above 50 suggest very strong trends where trend-following strategies may be more appropriate \cite{wilder1978new}. For mean reversion trading, low ADX values help identify range-bound markets where prices are likely to oscillate around a mean.

#### Bollinger Bands (BB)

Bollinger Bands are volatility bands placed above and below a moving average \cite{bollinger2002bollinger}. Developed by John Bollinger, these bands adapt to market volatility by expanding during volatile periods and contracting during calmer periods \cite{bollinger2002bollinger}.

The typical Bollinger Band configuration consists of three lines \cite{bollinger2002bollinger}. The middle band is a simple moving average (SMA), commonly calculated over 20 periods. The upper band is positioned at k standard deviations (typically k=2) above the middle band. The lower band is positioned at k standard deviations below the middle band. The formula for the upper band is SMA + (k × standard deviation) and for the lower band is SMA - (k × standard deviation) \cite{bollinger2002bollinger}.

For mean reversion strategies, Bollinger Bands provide visual representations of overbought and oversold conditions \cite{bollinger2002bollinger}. When prices touch or exceed the upper band, the asset may be overvalued relative to recent history, suggesting a potential selling opportunity. When prices touch or fall below the lower band, the asset may be undervalued, suggesting a potential buying opportunity \cite{bollinger2002bollinger}.

The width of the bands reflects market volatility \cite{bollinger2002bollinger}. Narrow bands indicate low volatility periods that often precede significant price movements. Wide bands indicate high volatility periods where prices have moved substantially from the mean. The "squeeze" occurs when bands narrow considerably, signaling potential breakouts, while "walking the band" happens when prices repeatedly touch the upper or lower band during strong trends \cite{bollinger2002bollinger}.

Statistical interpretation suggests that approximately 95% of price action should occur within two standard deviation bands under normal distribution assumptions \cite{bollinger2002bollinger}. In practice, this percentage varies depending on market conditions and the distribution of returns. For mean reversion trading, the key insight is that extreme deviations from the middle band are likely to be followed by reversals toward the mean.


# System Architecture

Our algorithmic trading system implements a distributed architecture entirely on Google Cloud Platform, leveraging serverless computing paradigms to achieve scalability, reliability, and cost efficiency. The architecture consists of six major components that orchestrate data ingestion, statistical screening, real-time monitoring, and signal generation. Figure 1 illustrates the complete system architecture showing data flow paths and component interactions.

## Architecture Overview

The system operates in two primary modes: **initialization/screening mode** for identifying suitable trading candidates, and **monitoring/execution mode** for generating trading signals. The architecture separates batch processing workloads from real-time stream processing, allowing each component to scale independently based on workload characteristics. Data flows through the system in a pipeline pattern, with each stage performing specific transformations and computations before passing results downstream.

From a distributed systems perspective, the architecture implements several key design patterns. **Event-driven architecture** enables loose coupling between components through asynchronous message passing via Pub/Sub \cite{fowler2002patterns}. **Separation of concerns** isolates data storage (BigQuery, Firestore), computation (Cloud Run, BigQuery UDFs), and monitoring (local client) into distinct components \cite{bass2012software}. **Polyglot persistence** stores different data types in specialized databases optimized for their access patterns—analytical queries in BigQuery and operational data in Firestore \cite{sadalage2012nosql}.

## Component 1: Initialization and Historical Data Ingestion

The initialization component establishes the foundation for the trading system by acquiring comprehensive historical market data. This one-time setup process retrieves two years of daily price data for all NASDAQ-listed securities from Yahoo Finance and loads it into BigQuery for subsequent analysis.

### Data Acquisition Process

The initialization process begins by enumerating all symbols traded on the NASDAQ exchange. For each symbol, the system fetches historical daily Open, High, Low, Close, and Volume (OHLCV) data spanning the previous two years with one-day intervals.

Data retrieval is implemented using the BigQuery Cloud API, which provides programmatic access to load data into BigQuery tables \cite{google_bigquery_api}. The API supports batch loading operations optimized for ingesting large datasets. Rather than making individual API calls for each symbol sequentially, the system implements **parallel data loading** by partitioning symbols into batches and processing multiple batches concurrently \cite{melnik2020dremel}. This parallelization significantly reduces total ingestion time from potentially days to hours.

### BigQuery Storage and Schema Design

The historical data is stored in a BigQuery table named **Daily Data Table** with a schema designed for efficient analytical queries. The table uses **partitioning** by date to optimize query performance and reduce costs \cite{google_bigquery_partitioning}. Date-based partitioning allows BigQuery to scan only relevant partitions when filtering by date ranges, dramatically improving query speed for time-series analysis \cite{melnik2020dremel}.

From a distributed systems perspective, BigQuery's storage layer (Colossus) automatically replicates data across multiple storage nodes within the region for durability and availability \cite{corbett2013spanner}. The separation of storage and compute in BigQuery allows the same data to be queried by multiple concurrent screening jobs without resource contention \cite{melnik2020dremel}.

## Component 2: Statistical Screening Query

The statistical screening component identifies securities exhibiting mean-reverting behavior suitable for the trading strategy. This component executes weekly to maintain an updated watchlist while avoiding excessive turnover that could destabilize trading performance.

### User-Defined Functions (UDFs) in BigQuery

The screening logic is implemented through two custom User-Defined Functions (UDFs) written in SQL and executed within BigQuery's distributed query engine \cite{google_bigquery_udf}. The **Hurst Exponent UDF** computes the Hurst exponent for each symbol's price time series, quantifying the degree of mean reversion or trending behavior \cite{hurst1951long}. The **Variance Ratio Test UDF** implements the heteroskedasticity-consistent variance ratio test to statistically validate mean-reverting characteristics \cite{lo1988stock}.

These UDFs accept arrays of historical prices as input and return numerical scores indicating mean reversion strength. Implementing these computations as UDFs enables **massive parallel processing** where BigQuery distributes the screening computation across thousands of worker nodes \cite{melnik2010dremel}. Each worker processes a subset of symbols independently, with no coordination required between workers since symbols are screened independently.

### Distributed Query Execution

The screening query follows a structure that scans the Daily Data Table, groups records by symbol, aggregates price arrays for each symbol, applies the Hurst and VR Test UDFs to each group, and filters symbols meeting mean reversion criteria. BigQuery's query planner automatically parallelizes this execution \cite{melnik2010dremel}.

During execution, BigQuery's **Dremel engine** decomposes the query into a tree of execution stages \cite{melnik2010dremel}. Leaf nodes scan partitions of the Daily Data Table in parallel, reading columnar data for relevant fields. Intermediate nodes aggregate price arrays by symbol, utilizing BigQuery's shuffle infrastructure to redistribute data so all records for each symbol land on the same worker. Root nodes apply the screening UDFs and collect results meeting the threshold criteria.

This tree-based execution achieves **horizontal scalability**—adding more workers linearly reduces query execution time until I/O bandwidth becomes the bottleneck \cite{melnik2010dremel}. For screening thousands of NASDAQ symbols, the query typically completes within minutes despite processing gigabytes of historical data.

### Scheduled Execution

The screening query is configured to execute automatically every week using **Cloud Scheduler**, a fully managed cron job service \cite{google_cloud_scheduler}. Screening occurs during weekends when markets are closed to ensure the watchlist is ready before Monday trading.

## Component 3: Watchlist Storage in Firestore

Screening results are stored in Firestore, a distributed NoSQL document database that serves as the central repository for operational trading data \cite{google_firestore_docs}. Firestore maintains the current **watchlist**, which contains the symbols that passed statistical screening and are actively monitored for trading opportunities.

The watchlist is modeled as a Firestore collection named **Watchlists** containing individual documents for each selected symbol. Each document stores the symbol identifier. This document-oriented structure provides flexibility to add new fields without schema migrations \cite{harrison2015nosql}.

After each weekly screening, the system updates Firestore with the latest results. The update process compares new screening results against the existing watchlist, adding newly qualified symbols and removing symbols that no longer meet criteria. 

## Component 4: Cloud Run Producer and Data Routing

The Cloud Run Producer orchestrates the retrieval of high-frequency price data for symbols on the watchlist and publishes this data to Pub/Sub topics for downstream consumption. This component serves as the **control plane** of the real-time monitoring system, coordinating data flow between various components.

### Dual-Mode Operation: Backtesting vs. Real-time

The Producer operates in two distinct modes depending on the system's operating context. In **backtesting mode**, the Producer retrieves historical 5-minute interval price data from the **5-minute BigQuery table** for strategy validation. In **real-time mode**, the Producer fetches live 5-minute price data directly from Yahoo Finance to enable actual trading signal generation.

### Real-time Fetcher Component

The **Real-time Fetcher** is a sub-component invoked by the Producer to retrieve current market data. In real-time mode, it makes HTTP requests to Yahoo Finance APIs with 5-minute intervals, requesting the latest price data for watchlist symbols \cite{perlin2020getSymbols}. The fetcher implements **rate limiting** to respect API usage quotas and avoid being throttled or banned by the data provider.

In backtesting mode, the Real-time Fetcher queries the 5-minute BigQuery table with 5-second intervals, which contains historical intraday price data pre-loaded during initialization or through separate data collection processes. 

### Cloud Run Deployment Model

The Producer is deployed as a Cloud Run service, Google's serverless container platform \cite{google_cloudrun_docs}. Cloud Run automatically scales the number of container instances based on incoming request volume, implementing **autoscaling** based on concurrency and CPU utilization metrics \cite{google_cloudrun_docs}. When monitoring is inactive, Cloud Run scales to zero instances, eliminating idle resource costs.

Each Producer instance runs as a **stateless container**, storing no persistent data locally. This statelessness enables Cloud Run to freely create, destroy, and migrate instances across the infrastructure without coordination \cite{burns2018kubernetes}.

### Scheduled Execution Trigger

The Producer is triggered on a fixed schedule (every 5 seconds) by **Cloud Scheduler** during market hours \cite{google_cloud_scheduler}. Each trigger initiates a new request to the Producer's HTTP endpoint, which causes Cloud Run to instantiate a container instance if none are available (cold start) or route to an existing warm instance.

### Publishing to Pub/Sub

After retrieving price data, the Producer publishes messages to Google Cloud Pub/Sub topics, one message per symbol containing the latest OHLCV data. Each message includes the symbol identifier, timestamp, and price information serialized in JSON format.

Pub/Sub topics are organized by symbol, creating a separate topic for each watchlist symbol (e.g., "AAPL", "MSFT"). This **topic-per-symbol** architecture enables fine-grained subscription control where monitors can subscribe only to symbols of interest. However, it increases management overhead for large watchlists. An alternative **single-topic** approach publishes all symbols to one topic with symbol identifiers in message attributes, allowing subscribers to filter messages based on attributes.

Publishing to Pub/Sub provides **temporal decoupling** between data production and consumption \cite{eugster2003many}. The Producer does not need to know about downstream consumers, and consumers can process messages at their own pace without affecting the Producer. Pub/Sub buffers messages for up to 7 days by default, ensuring message delivery even if subscribers are temporarily offline \cite{google_pubsub_docs}.

## Component 5: Pub/Sub Message Queue

Google Cloud Pub/Sub serves as the **message-oriented middleware** connecting the Producer with downstream monitoring components. It implements the publish-subscribe pattern, enabling **asynchronous communication** and **loose coupling** between system components \cite{eugster2003many}.

## Component 6: Local Monitor and Signal Generation

The Monitor component subscribes to Pub/Sub topics, receives real-time price data, computes technical indicators (ADX and Bollinger Bands), and generates buy-in signals when conditions are met. This component runs on a local computer rather than in the cloud to provide direct control and immediate notification of trading opportunities.

### Pub/Sub Subscription Model

The Monitor establishes a **pull subscription** to Pub/Sub topics for symbols on the watchlist \cite{google_pubsub_docs}. In pull subscriptions, the subscriber actively requests messages from Pub/Sub servers using API calls. The Monitor implements a continuous polling loop that calls the Pull API every few seconds, receives batches of messages, processes them, and acknowledges successful processing.

### Signal Generation Logic

After computing indicators, the Monitor evaluates **cross conditions** to determine if trading signals should be generated. The strategy uses a two-filter approach: ADX indicates whether the market is suitable for mean reversion trading (low ADX values suggest range-bound behavior), and Bollinger Bands identify entry points (price crossing below the lower band suggests oversold conditions ripe for reversal) \cite{chan2009quantitative}.

The specific logic checks if ADX is below a threshold (e.g., ADX < 25) indicating weak trend strength, and if the current price crosses below the lower Bollinger Band \cite{bollinger2002bollinger}. When both conditions are met simultaneously, the Monitor generates a **buy-in signal** that can trigger automated order execution or notify human traders.

## Daily Fetcher Component

To maintain current market data for screening, a separate pipeline updates the Daily Data Table in BigQuery with the latest daily prices. This pipeline operates independently of the real-time monitoring system, ensuring that long-term data remains fresh for weekly screening.

The **Daily Fetcher** is a scheduled Cloud Run service that executes every midnight after markets close. It retrieves daily OHLCV data for all NASDAQ-listed symbols from Yahoo Finance, covering the most recent trading day. The fetcher implements similar data retrieval logic as the initialization component but processes only a single day's data rather than two years.


















