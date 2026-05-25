package storage

import "fmt"

// otel_metric_series is the shared metadata catalogue for every metric series.
// One row per unique (MetricName, MetricType, ResourceAttributes, ScopeName,
// ScopeVersion, datapoint Attributes) combination. Datapoints reference a row
// here by SeriesID (xxhash fingerprint computed in the ingest layer).
//
// ReplacingMergeTree(LastSeen) makes repeated inserts of the same series
// idempotent: ClickHouse deduplicates by ORDER BY key (SeriesID) during
// background merges, keeping the row with the latest LastSeen. No locking
// required on the write path.
//
// Bloom filters on attribute keys/values + service+metric name live here
// because attributes no longer live on the datapoints tables. Queries that
// filter by attribute hit this table first (bloom -> SeriesIDs), then fetch
// from the relevant datapoints table.
const createSeriesTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metric_series (
    SeriesID              UInt64                              CODEC(ZSTD(1)),
    MetricType            LowCardinality(String)              CODEC(ZSTD(1)),
    ServiceName           LowCardinality(String)              CODEC(ZSTD(1)),
    MetricName            LowCardinality(String)              CODEC(ZSTD(1)),
    MetricDescription     String                              CODEC(ZSTD(1)),
    MetricUnit            String                              CODEC(ZSTD(1)),
    ResourceAttributes    Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ResourceSchemaUrl     String                              CODEC(ZSTD(1)),
    ScopeName             LowCardinality(String)              CODEC(ZSTD(1)),
    ScopeVersion          LowCardinality(String)              CODEC(ZSTD(1)),
    ScopeAttributes       Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount UInt32                              CODEC(ZSTD(1)),
    ScopeSchemaUrl        String                              CODEC(ZSTD(1)),
    Attributes            Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    FirstSeen             DateTime                            CODEC(ZSTD(1)),
    LastSeen              DateTime                            CODEC(ZSTD(1)),

    INDEX idx_service_name     ServiceName                    TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_metric_name      MetricName                     TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_key     mapKeys(ResourceAttributes)    TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value   mapValues(ResourceAttributes)  TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key   mapKeys(ScopeAttributes)       TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_value mapValues(ScopeAttributes)     TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key         mapKeys(Attributes)            TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_value       mapValues(Attributes)          TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = ReplacingMergeTree(LastSeen)
ORDER BY SeriesID
SETTINGS index_granularity = 8192;
`

// Datapoints tables hold only the time-varying signal: SeriesID (link to the
// metadata catalogue), timestamps, value, type-specific columns.
//
// ORDER BY (SeriesID, TimeUnix) clusters all points for one series together
// and orders them by time within that cluster — the natural shape for the
// "give me this series over time" query. PARTITION BY toDate(TimeUnix)
// confines time-range scans to a small set of partitions.

const createGaugeTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_gauge (
    SeriesID      UInt64        CODEC(ZSTD(1)),
    StartTimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    TimeUnix      DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    Value         Float64       CODEC(ZSTD(1)),
    Flags         UInt32        CODEC(ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (SeriesID, toUnixTimestamp64Nano(TimeUnix))
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`

const createSumTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_sum (
    SeriesID               UInt64        CODEC(ZSTD(1)),
    StartTimeUnix          DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    TimeUnix               DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    Value                  Float64       CODEC(ZSTD(1)),
    Flags                  UInt32        CODEC(ZSTD(1)),
    AggregationTemporality Int32         CODEC(ZSTD(1)),
    IsMonotonic            Bool          CODEC(ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (SeriesID, toUnixTimestamp64Nano(TimeUnix))
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`

const createHistogramTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_histogram (
    SeriesID               UInt64        CODEC(ZSTD(1)),
    StartTimeUnix          DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    TimeUnix               DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    Count                  UInt64        CODEC(Delta(8), ZSTD(1)),
    Sum                    Float64       CODEC(ZSTD(1)),
    BucketCounts           Array(UInt64) CODEC(ZSTD(1)),
    ExplicitBounds         Array(Float64) CODEC(ZSTD(1)),
    Min                    Float64       CODEC(ZSTD(1)),
    Max                    Float64       CODEC(ZSTD(1)),
    Flags                  UInt32        CODEC(ZSTD(1)),
    AggregationTemporality Int32         CODEC(ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (SeriesID, toUnixTimestamp64Nano(TimeUnix))
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`

const createExponentialHistogramTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_exponential_histogram (
    SeriesID               UInt64        CODEC(ZSTD(1)),
    StartTimeUnix          DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    TimeUnix               DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    Count                  UInt64        CODEC(Delta(8), ZSTD(1)),
    Sum                    Float64       CODEC(ZSTD(1)),
    Scale                  Int32         CODEC(ZSTD(1)),
    ZeroCount              UInt64        CODEC(ZSTD(1)),
    PositiveOffset         Int32         CODEC(ZSTD(1)),
    PositiveBucketCounts   Array(UInt64) CODEC(ZSTD(1)),
    NegativeOffset         Int32         CODEC(ZSTD(1)),
    NegativeBucketCounts   Array(UInt64) CODEC(ZSTD(1)),
    Min                    Float64       CODEC(ZSTD(1)),
    Max                    Float64       CODEC(ZSTD(1)),
    Flags                  UInt32        CODEC(ZSTD(1)),
    AggregationTemporality Int32         CODEC(ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (SeriesID, toUnixTimestamp64Nano(TimeUnix))
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`

const createSummaryTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_summary (
    SeriesID      UInt64        CODEC(ZSTD(1)),
    StartTimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    TimeUnix      DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    Count         UInt64        CODEC(Delta(8), ZSTD(1)),
    Sum           Float64       CODEC(ZSTD(1)),
    ValueAtQuantiles Nested(
        Quantile Float64,
        Value    Float64
    ) CODEC(ZSTD(1)),
    Flags         UInt32        CODEC(ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (SeriesID, toUnixTimestamp64Nano(TimeUnix))
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`

// kafkaTableDef describes a Kafka engine queue table and its corresponding
// materialized view that bridges rows into an existing destination table.
type kafkaTableDef struct {
	queueTable  string
	destTable   string
	topicSuffix string
	columns     string
}

var kafkaTableDefs = []kafkaTableDef{
	{
		queueTable:  "otel_metric_series_queue",
		destTable:   "otel_metric_series",
		topicSuffix: "series",
		columns: `
    SeriesID              UInt64,
    MetricType            LowCardinality(String),
    ServiceName           LowCardinality(String),
    MetricName            LowCardinality(String),
    MetricDescription     String,
    MetricUnit            String,
    ResourceAttributes    Map(LowCardinality(String), String),
    ResourceSchemaUrl     String,
    ScopeName             LowCardinality(String),
    ScopeVersion          LowCardinality(String),
    ScopeAttributes       Map(LowCardinality(String), String),
    ScopeDroppedAttrCount UInt32,
    ScopeSchemaUrl        String,
    Attributes            Map(LowCardinality(String), String),
    FirstSeen             DateTime,
    LastSeen              DateTime`,
	},
	{
		queueTable:  "otel_metrics_gauge_queue",
		destTable:   "otel_metrics_gauge",
		topicSuffix: "gauge",
		columns: `
    SeriesID      UInt64,
    StartTimeUnix DateTime64(9),
    TimeUnix      DateTime64(9),
    Value         Float64,
    Flags         UInt32`,
	},
	{
		queueTable:  "otel_metrics_sum_queue",
		destTable:   "otel_metrics_sum",
		topicSuffix: "sum",
		columns: `
    SeriesID               UInt64,
    StartTimeUnix          DateTime64(9),
    TimeUnix               DateTime64(9),
    Value                  Float64,
    Flags                  UInt32,
    AggregationTemporality Int32,
    IsMonotonic            Bool`,
	},
	{
		queueTable:  "otel_metrics_histogram_queue",
		destTable:   "otel_metrics_histogram",
		topicSuffix: "histogram",
		columns: `
    SeriesID               UInt64,
    StartTimeUnix          DateTime64(9),
    TimeUnix               DateTime64(9),
    Count                  UInt64,
    Sum                    Float64,
    BucketCounts           Array(UInt64),
    ExplicitBounds         Array(Float64),
    Min                    Float64,
    Max                    Float64,
    Flags                  UInt32,
    AggregationTemporality Int32`,
	},
	{
		queueTable:  "otel_metrics_exponential_histogram_queue",
		destTable:   "otel_metrics_exponential_histogram",
		topicSuffix: "exponential_histogram",
		columns: `
    SeriesID               UInt64,
    StartTimeUnix          DateTime64(9),
    TimeUnix               DateTime64(9),
    Count                  UInt64,
    Sum                    Float64,
    Scale                  Int32,
    ZeroCount              UInt64,
    PositiveOffset         Int32,
    PositiveBucketCounts   Array(UInt64),
    NegativeOffset         Int32,
    NegativeBucketCounts   Array(UInt64),
    Min                    Float64,
    Max                    Float64,
    Flags                  UInt32,
    AggregationTemporality Int32`,
	},
	{
		queueTable:  "otel_metrics_summary_queue",
		destTable:   "otel_metrics_summary",
		topicSuffix: "summary",
		columns: `
    SeriesID      UInt64,
    StartTimeUnix DateTime64(9),
    TimeUnix      DateTime64(9),
    Count         UInt64,
    Sum           Float64,
    ValueAtQuantiles Nested(
        Quantile Float64,
        Value    Float64
    ),
    Flags         UInt32`,
	},
}

func queueTableDDL(def kafkaTableDef, brokers, topicPrefix string) string {
	topic := topicPrefix + "." + def.topicSuffix
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s
) ENGINE = Kafka
SETTINGS kafka_broker_list = '%s',
         kafka_topic_list = '%s',
         kafka_group_name = 'clickhouse.%s',
         kafka_format = 'JSONEachRow',
         kafka_max_block_size = 65536`,
		def.queueTable, def.columns, brokers, topic, topic)
}

func mvDDL(def kafkaTableDef) string {
	mvName := def.destTable + "_mv"
	return fmt.Sprintf(
		`CREATE MATERIALIZED VIEW IF NOT EXISTS %s TO %s AS SELECT * FROM %s`,
		mvName, def.destTable, def.queueTable)
}