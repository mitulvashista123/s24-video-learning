"""Service 8 – Usage Analytics Service (Apache Flink / PyFlink on Amazon EMR).

Real-time, stateful, event-time stream processing:

  sources : Kafka `learner-progress-events`  (Progress Tracker)
            Kafka `content-access-events`    (Content Delivery API: cache hit/miss/stale)
  process : union into one schema, assign event-time watermarks (10 s bounded
            out-of-orderness), then a 1-minute TUMBLING window per item with
            COUNT(DISTINCT user_id) (keyed, windowed state) plus views,
            completions, watch time, cache hits / misses and stale reads
  sink    : Kafka `usage-analytics-results`  (consumed by the Recommendation
            Lambda, which stores it in DynamoDB `UsageAnalytics`)

Run on EMR (submitted by Terraform as an EMR step):
    flink run -t yarn-per-job -py flink_job.py --bootstrap-servers b-1...:9092,b-2...:9092
Run locally / in the container image:
    flink run -py /opt/s24/flink_job.py --bootstrap-servers kafka:9092
"""

import argparse
import logging
import os
import sys

from pyflink.common import Configuration
from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

logging.basicConfig(
    stream=sys.stdout, level=logging.INFO,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","service":"usage-analytics-flink","msg":"%(message)s"}',
)
log = logging.getLogger("usage-analytics-flink")

CONNECTOR_CANDIDATES = [
    "/opt/s24/flink-sql-connector-kafka.jar",           # EMR bootstrap action
    "/opt/flink/usrlib/flink-sql-connector-kafka.jar",  # container image
]


def parse_args(argv):
    p = argparse.ArgumentParser(description="S24 usage analytics – 1-minute tumbling window aggregation")
    p.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    p.add_argument("--source-topics",
                   default="learner-progress-events,content-access-events",
                   help="progress topic,access topic")
    p.add_argument("--sink-topic", default=os.getenv("TOPIC_ANALYTICS_RESULTS", "usage-analytics-results"))
    p.add_argument("--group-id", default="s24-usage-analytics")
    p.add_argument("--window-minutes", type=int, default=1)
    p.add_argument("--out-of-orderness-seconds", type=int, default=10)
    p.add_argument("--parallelism", type=int, default=2)
    p.add_argument("--checkpoint-interval-ms", type=int, default=30000)
    p.add_argument("--starting-offsets", default="latest-offset",
                   choices=["latest-offset", "earliest-offset", "group-offsets"])
    args, _unknown = p.parse_known_args(argv)
    return args


def build_environment(args) -> StreamTableEnvironment:
    config = Configuration()
    jars = [f"file://{j}" for j in CONNECTOR_CANDIDATES if os.path.exists(j)]
    if jars:
        config.set_string("pipeline.jars", ";".join(jars))
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(args.parallelism)
    env.enable_checkpointing(args.checkpoint_interval_ms, CheckpointingMode.EXACTLY_ONCE)
    t_env = StreamTableEnvironment.create(env)
    cfg = t_env.get_config()
    cfg.set("pipeline.name", "s24-usage-analytics")
    cfg.set("table.local-time-zone", "UTC")
    # A partition/topic with no traffic must not hold back the watermark forever.
    cfg.set("table.exec.source.idle-timeout", "30 s")
    # State for COUNT(DISTINCT) lives only for the window; keep a safety TTL anyway.
    cfg.set("table.exec.state.ttl", "10 min")
    return t_env


def kafka_source_options(topic: str, args) -> str:
    return f"""
        'connector' = 'kafka',
        'topic' = '{topic}',
        'properties.bootstrap.servers' = '{args.bootstrap_servers}',
        'properties.group.id' = '{args.group_id}',
        'scan.startup.mode' = '{args.starting_offsets}',
        'format' = 'json',
        'json.ignore-parse-errors' = 'true',
        'json.fail-on-missing-field' = 'false'
    """


def define_pipeline(t_env: StreamTableEnvironment, args) -> None:
    progress_topic, access_topic = [t.strip() for t in args.source_topics.split(",")]
    ooo = args.out_of_orderness_seconds

    # ------------------------------------------------------------------ sources
    t_env.execute_sql(f"""
        CREATE TABLE progress_events (
            event_id               STRING,
            user_id                STRING,
            lesson_id              STRING,
            course_id              STRING,
            `event`                STRING,
            `percent`              DOUBLE,
            watched_delta_seconds  DOUBLE,
            newly_completed        BOOLEAN,
            event_time             BIGINT,
            ts AS TO_TIMESTAMP_LTZ(event_time, 3),
            WATERMARK FOR ts AS ts - INTERVAL '{ooo}' SECOND
        ) WITH ({kafka_source_options(progress_topic, args)})
    """)

    t_env.execute_sql(f"""
        CREATE TABLE access_events (
            event_id    STRING,
            event_type  STRING,
            `resource`  STRING,
            item_id     STRING,
            course_id   STRING,
            user_id     STRING,
            cache_hit   BOOLEAN,
            stale       BOOLEAN,
            event_time  BIGINT,
            ts AS TO_TIMESTAMP_LTZ(event_time, 3),
            WATERMARK FOR ts AS ts - INTERVAL '{ooo}' SECOND
        ) WITH ({kafka_source_options(access_topic, args)})
    """)

    # ------------------------------------------------------------------ sink
    t_env.execute_sql(f"""
        CREATE TABLE usage_results (
            `item_id`        STRING,
            `course_id`      STRING,
            `window_start`   STRING,
            `window_end`     STRING,
            `unique_users`   BIGINT,
            `views`          BIGINT,
            `completions`    BIGINT,
            `progress_events`  BIGINT,
            `watch_seconds`  DOUBLE,
            `cache_hits`     BIGINT,
            `cache_misses`   BIGINT,
            `stale_probes`   BIGINT,
            `stale_reads`    BIGINT,
            `cache_hit_ratio`  DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{args.sink_topic}',
            'properties.bootstrap.servers' = '{args.bootstrap_servers}',
            'key.format' = 'raw',
            'key.fields' = 'item_id',
            'value.format' = 'json',
            'value.fields-include' = 'ALL',
            'sink.delivery-guarantee' = 'at-least-once'
        )
    """)

    # ------------------------------------------------------------------ unify both streams
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW unified_events AS
        SELECT
            lesson_id                                                         AS item_id,
            COALESCE(course_id, '')                                           AS course_id,
            user_id,
            CASE WHEN `event` = 'play' THEN 1 ELSE 0 END                        AS is_view,
            CASE WHEN newly_completed THEN 1 ELSE 0 END                       AS is_completion,
            1                                                                 AS is_progress,
            COALESCE(watched_delta_seconds, CAST(0 AS DOUBLE))                AS watch_seconds,
            0 AS is_hit, 0 AS is_miss, 0 AS is_probe, 0 AS is_stale,
            ts
        FROM progress_events
        WHERE lesson_id IS NOT NULL AND user_id IS NOT NULL
        UNION ALL
        SELECT
            item_id,
            COALESCE(course_id, ''),
            user_id,
            0, 0, 0, CAST(0 AS DOUBLE),
            CASE WHEN event_type = 'access' AND cache_hit THEN 1 ELSE 0 END,
            CASE WHEN event_type = 'access' AND NOT cache_hit THEN 1 ELSE 0 END,
            CASE WHEN event_type = 'stale_probe' THEN 1 ELSE 0 END,
            CASE WHEN event_type = 'stale_probe' AND stale THEN 1 ELSE 0 END,
            ts
        FROM access_events
        WHERE item_id IS NOT NULL AND user_id IS NOT NULL
    """)

    # ------------------------------------------------------------------ stateful 1-minute tumbling window
    window = args.window_minutes
    statement = t_env.create_statement_set()
    statement.add_insert_sql(f"""
        INSERT INTO usage_results
        SELECT
            item_id,
            MAX(course_id)                                                        AS course_id,
            DATE_FORMAT(window_start, 'yyyy-MM-dd''T''HH:mm:ss''Z''')             AS window_start,
            DATE_FORMAT(window_end,   'yyyy-MM-dd''T''HH:mm:ss''Z''')             AS window_end,
            COUNT(DISTINCT user_id)                                               AS unique_users,
            CAST(SUM(is_view) AS BIGINT)                                          AS `views`,
            CAST(SUM(is_completion) AS BIGINT)                                    AS completions,
            CAST(SUM(is_progress) AS BIGINT)                                      AS progress_events,
            ROUND(SUM(watch_seconds), 2)                                          AS watch_seconds,
            CAST(SUM(is_hit) AS BIGINT)                                           AS cache_hits,
            CAST(SUM(is_miss) AS BIGINT)                                          AS cache_misses,
            CAST(SUM(is_probe) AS BIGINT)                                         AS stale_probes,
            CAST(SUM(is_stale) AS BIGINT)                                         AS stale_reads,
            CASE WHEN SUM(is_hit) + SUM(is_miss) = 0 THEN CAST(NULL AS DOUBLE)
                 ELSE ROUND(CAST(SUM(is_hit) AS DOUBLE) / (SUM(is_hit) + SUM(is_miss)), 4)
            END                                                                   AS cache_hit_ratio
        FROM TABLE(
            TUMBLE(TABLE unified_events, DESCRIPTOR(ts), INTERVAL '{window}' MINUTE)
        )
        GROUP BY item_id, window_start, window_end
    """)

    log.info(
        "submitting job: sources=%s,%s sink=%s window=%dmin watermark_delay=%ds brokers=%s",
        progress_topic, access_topic, args.sink_topic, window, ooo, args.bootstrap_servers,
    )
    statement.execute()


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    t_env = build_environment(args)
    define_pipeline(t_env, args)


if __name__ == "__main__":
    main()
