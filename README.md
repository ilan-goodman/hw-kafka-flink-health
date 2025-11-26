# HW: Real-Time Health Monitoring with Kafka and Flink

## Overview

In this assignment you will build a **small real‑time data pipeline** to monitor synthetic heart‑rate data for hospital patients.

You will:

- Produce **streaming heart‑rate events** into **Kafka**.
- Implement a **PyFlink streaming job** that:
  - Consumes events from Kafka.
  - Parses and validates JSON messages.
  - Uses **event time** and **watermarks**.
  - Computes **rolling statistics** (avg/min/max) per patient in windows.
  - Flags abnormal readings (e.g., tachycardia, bradycardia) in near real time.
- (Optional but recommended) Run a Spark batch job over the alerts.

The focus is on **Kafka + Flink**. Spark and Snowflake are included only as optional context/extensions.

You will run everything on the school‑managed Linux machines (“LinuxLab”).

---

## Learning Objectives

After completing this assignment, you should be able to:

1. Start and use a local Kafka cluster (single node) on LinuxLab.
2. Use Python to produce JSON messages into a Kafka topic.
3. Write and run a **PyFlink** streaming job that:
   - Uses Kafka as a source and sink.
   - Uses event‑time and watermarks for correctness.
   - Maintains per‑key (per‑patient) state and performs windowed aggregations.
   - Emits alert messages for abnormal conditions.
4. Write simple unit tests for your parsing / classification logic.
5. (Optional) Use Spark to perform batch analysis of Flink’s output.

---

## Scenario / Domain Context

You are working for a fictional hospital network. Patients wear continuous heart‑rate monitors that generate events:

```json
{
  "patient_id": "p123",
  "timestamp": "2025-11-19T07:15:23.123Z",
  "heart_rate_bpm": 94
}
```

The hospital wants a **real‑time monitoring system** that:

- Tracks heart rate for each patient over short time windows.
- Detects abnormal averages:
  - **Tachycardia:** `avg_hr > 100 bpm`
  - **Bradycardia:** `avg_hr < 50 bpm`
- Emits **alerts** that can be consumed by other systems (e.g., dashboards, pagers, or downstream analytics).

You will **simulate** this data (no real PHI).

---

## Repository Layout

This repo contains:

```text
hw-kafka-flink-health/
  README-student.md          # this file
  env.sh                     # optional helper for environment variables
  requirements.txt           # Python dependencies

  src/
    kafka_producer.py        # starter Kafka producer (you may extend)
    flink_job.py             # PyFlink streaming job (you implement core logic)
    spark_batch_analysis.py  # optional Spark batch analysis script

  tests/
    test_kafka_payload.py    # tests for Kafka payload generation
    test_flink_logic.py      # tests for parse_event() & classify_window()

  # NOTE: there is a separate instructor repo with reference solutions.
  # You do NOT have access to that repo.
```

You will primarily modify:

- `src/flink_job.py`
- (optionally) `src/kafka_producer.py`
- (optionally) `src/spark_batch_analysis.py`

---

## Environment Setup on LinuxLab

### 1. Log in

From your local machine:

```bash
ssh <your_wustl_id>@shell.engr.wustl.edu
```

Clone or copy this repo into your home directory.

### 2. Environment variables (shared tools vs. your Python venv)

On LinuxLab we will use:

- **Centrally managed** installations for:
  - Kafka under `/opt/kafka`
  - Flink under `/opt/flink`
  - Spark under `/opt/spark`
- A **per-student Python virtualenv** in this repo for your homework code.

If provided, source the helper script:

```bash
cd ~/hw-kafka-flink-health
source env.sh
```

If `env.sh` does not exist or paths differ, set them manually (ask the instructor/TA for the correct paths):

```bash
export KAFKA_HOME=/opt/kafka
export FLINK_HOME=/opt/flink
export SPARK_HOME=/opt/spark
export PATH="$KAFKA_HOME/bin:$FLINK_HOME/bin:$SPARK_HOME/bin:$PATH"
```

Check versions:

```bash
$KAFKA_HOME/bin/kafka-topics.sh --version
$FLINK_HOME/bin/flink --version
$SPARK_HOME/bin/spark-shell --version
```

> **Typical workflow:**  
> - Use one terminal (with these environment variables) to start **Kafka** and **Flink** using the shared `/opt/...` installations.  
> - Use another terminal where you activate your **Python venv** (see below) to run `python`, `pytest`, and `flink run -py ...` for this homework.

### 3. Python virtual environment and dependencies

#### 3.1. On LinuxLab

On the `server-airflow25` (or as instructed by the course):

```bash
cd ~/hw-kafka-flink-health

python3 -m venv venv
source venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt
```

Check that `apache-flink`, `kafka-python`, and `pyspark` (optional) are installed **inside this venv**:

```bash
pip show apache-flink
pip show kafka-python
pip show pyspark   # optional, for Spark part
```

Whenever you work on this homework, activate the venv first:

```bash
cd ~/hw-kafka-flink-health
source venv/bin/activate
```

Then run `python src/...`, `pytest`, and `flink run -py ...` from this venv.

#### 3.2. If running locally

If you are doing the assignment on your own machine (with local Kafka/Flink installs), you can reuse the same venv pattern:

```bash
cd ~/hw-kafka-flink-health
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

## Starting Kafka

You will use **one** broker and **one** KRaft instance for this assignment (no ZooKeeper).

> **Note:** The storage format step below is a **one-time cluster bootstrap** for the broker’s data directory. On a shared cluster this may already have been done by IT; follow course-specific instructions about whether you should run it yourself.

Assuming KRaft-based Kafka:

### 1. Start Kafka in KRaft mode (one time only)

Only run this **once per broker data directory**, and only if instructed by the course staff:

```bash
# ONE-TIME ONLY (cluster bootstrap; do NOT re-run if already formatted)
CLUSTER_ID=$(python - << 'EOF'
import uuid
print(uuid.uuid4())
EOF
)

$KAFKA_HOME/bin/kafka-storage.sh format \
  -t "$CLUSTER_ID" \
  -c $KAFKA_HOME/config/server.properties \
  --standalone
```

### 2. Start Kafka broker

In a terminal where the shared environment is configured:

```bash
export KAFKA_HOME=/opt/kafka
export PATH="$KAFKA_HOME/bin:$PATH"

export LOG_DIR=~/kafka_logs

$KAFKA_HOME/bin/kafka-server-start.sh \
  $KAFKA_HOME/config/server.properties \
  > $HOME/kafka.log 2>&1 &
```

Check that the broker is running by listing topics:

```bash
$KAFKA_HOME/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

If this returns (even an empty list) without:

```text
Connection to node -1 (localhost/127.0.0.1:9092) could not be established
```

then Kafka has started.

You can also double-check the process:

```bash
ps aux | grep -E 'kafka\.server|KafkaRaftServer'
```

You should see a `java` process with a class name like `kafka.server.KafkaRaftServer`.

### 3. Create topics

Create the input and output topics:

```bash
$KAFKA_HOME/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic heart_rate_events \
  --partitions 3 --replication-factor 1

$KAFKA_HOME/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic heart_rate_alerts \
  --partitions 3 --replication-factor 1
```

Verify:

```bash
$KAFKA_HOME/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

You should see `heart_rate_events` and `heart_rate_alerts`.

---

## Starting Flink

Start a standalone **local Flink cluster** (using the centrally managed install):

```bash
$FLINK_HOME/bin/start-cluster.sh
```

Check that JobManager and TaskManager are running:

```bash
jps   # should show 'StandaloneSessionClusterEntrypoint' or similar
```

(Optional, if using a remote machine that's not LinuxLab) Forward the Flink web UI to your laptop:

```bash
ssh -L 8081:localhost:8081 <remote_userid>@<url>
# Then open http://localhost:8081 in your local browser.
```

> **Flink Kafka connector note:**  
> For `KafkaSource` / `KafkaSink` in PyFlink to work, the Flink Kafka connector JAR (e.g., `flink-connector-kafka-<version>.jar`) must be present on the cluster’s classpath.  
> If you see an error like:
>
> ```text
> TypeError: Could not found the Java class 'org.apache.flink.connector.kafka.source.KafkaSource.builder'
> ```
>
> it means the Kafka connector is missing. Follow course instructions or contact IT. Once the jar is installed (typically under `/opt/flink/opt`), you may need to add a `--jarfile /opt/flink/opt/flink-connector-kafka-<version>.jar` flag when running `flink run`.

---

## Quick Hello-World Check (Run This First)

Before you start the main assignment, please run this tiny **hello-world Kafka + Flink** example to make sure your environment is working.

### Step 0: Make sure Kafka and Flink are running

Follow the earlier instructions in this README to:

1. Start KRaft and the Kafka broker.
2. Start the Flink cluster (`$FLINK_HOME/bin/start-cluster.sh`).

### Step 1: Create hello-world topics

```bash
$KAFKA_HOME/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic hello_input \
  --partitions 1 --replication-factor 1

$KAFKA_HOME/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic hello_output \
  --partitions 1 --replication-factor 1
```

### Step 2: Run the hello-world Kafka producer

From the repo root (with your virtualenv activated):

```bash
cd ~/hw-kafka-flink-health
source venv/bin/activate
python src/hello_kafka_producer.py
```

You should see it send 10 small JSON messages to the `hello_input` topic.

### Step 3: Run the hello-world Flink job

In a separate terminal (Kafka + Flink still running, venv activated):

```bash
cd ~/hw-kafka-flink-health
source venv/bin/activate

$FLINK_HOME/bin/flink run -py src/hello_flink_job.py \
  --job_name "HelloFlinkJob"
```

> If the Kafka connector is not yet on the cluster classpath, your TA/IT may instruct you to add a `--jarfile` argument, for example:
>
> ```bash
> $FLINK_HOME/bin/flink run \
>   -py src/hello_flink_job.py \
>   --job_name "HelloFlinkJob" \
>   --jarfile /opt/flink/opt/flink-connector-kafka-<version>.jar
> ```
>
> Use the path/version provided by the instructor.

This job reads from `hello_input`, adds a `"processed_by": "hello_flink_job"` field, and writes to `hello_output`.

### Step 4: Inspect the output

In another terminal, consume from `hello_output`:

```bash
$KAFKA_HOME/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic hello_output \
  --from-beginning
```

You should see output similar to:

```json
{"id": 0, "message": "hello flink", "timestamp": "...", "processed_by": "hello_flink_job"}
{"id": 1, "message": "hello flink", "timestamp": "...", "processed_by": "hello_flink_job"}
...
```

If you see these messages:

- Kafka is running.
- The Python Kafka producer works.
- PyFlink can read from Kafka, transform data, and write back to Kafka.

Once this hello-world example works, you are ready to start the **real health monitoring assignment**.

---

## Part 1: Kafka Producer (Synthetic Health Data)

File: `src/kafka_producer.py`

### What this script should do

- Connect to `localhost:9092`.
- Produce JSON messages to topic `heart_rate_events`.
- Each message must have:
  - `patient_id` (string, like `"p001"`).
  - `timestamp` (ISO8601 with timezone, e.g. `"2025-11-19T07:15:23.123456+00:00"`).
  - `heart_rate_bpm` (integer).
- Send messages continuously (with a short sleep, e.g. 0.1 s) for multiple patients.

The starter code:

- Already uses `kafka-python`.
- Already defines a `generate_heart_rate_event(patient_id)` helper.
- You may extend it to:
  - simulate different baselines for each patient,
  - inject abnormal episodes (very high or very low HR).

### How to run it

From the repo root (inside your venv):

```bash
cd ~/hw-kafka-flink-health
source venv/bin/activate
python src/kafka_producer.py
```

You should see log output like “Starting heart rate event generation…”.

To **inspect messages**:

```bash
$KAFKA_HOME/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic heart_rate_events \
  --from-beginning
```

---

## Part 2: Flink Streaming Job (Core of the Assignment)

File: `src/flink_job.py`

You must implement:

1. `parse_event(value: str) -> dict | None`
2. `classify_window(avg_hr: float) -> str`
3. The **windowed aggregation pipeline**.

### 2.1. `parse_event`

**Required behavior** (see `tests/test_flink_logic.py`):

- Input: raw JSON string from Kafka.
- Output:
  - On success: a dict with keys:
    - `"patient_id"` (string),
    - `"heart_rate_bpm"` (int),
    - `"event_time"` (int epoch milliseconds).
  - On any parsing/validation error: `None`.

**Hints:**

- Use `json.loads`.
- Validate that `patient_id`, `timestamp`, and `heart_rate_bpm` are present.
- Use `datetime.fromisoformat(...)` to parse the timestamp.
- Convert to epoch milliseconds via `int(dt.timestamp() * 1000)`.
- Keep this function **pure** (no Flink dependencies) so it’s easy to unit test.

### 2.2. `classify_window`

**Required behavior:**

- Input: window average heart rate (float).
- Output: `"tachycardia"`, `"bradycardia"`, or `"normal"`, using thresholds:
  - `avg_hr > 100` → `"tachycardia"`
  - `avg_hr < 50` → `"bradycardia"`
  - otherwise `"normal"`

See `tests/test_flink_logic.py` for exact expectations.

### 2.3. Streaming pipeline

You must complete the `main()` function so that:

1. Flink consumes from `heart_rate_events` using a `KafkaSource` (already in the starter).
2. You:
   - Parse the JSON strings with `parse_event`.
   - Filter out invalid events (`None`).
3. Use **event time** and **watermarks**:
   - Watermark strategy with bounded out‑of‑orderness, e.g. 5 seconds.
   - Timestamps should come from the `event_time` field you created.
4. Group and window:
   - Key by `patient_id`.
   - Use a **time window**:
     - For example: 1‑minute tumbling windows (`Time.minutes(1)`), or
     - A sliding window (e.g., slide of 10 seconds over 1 minute).
   - Compute for each window:
     - `avg_hr`, `min_hr`, `max_hr`.
5. Classify:
   - Use `classify_window(avg_hr)` to produce an `alert_type`.
6. Emit alerts:
   - Generate an alert record as a JSON string with fields:
     - `patient_id`
     - `window_start` (epoch millis or ISO string)
     - `window_end`
     - `avg_hr`
     - `min_hr`
     - `max_hr`
     - `alert_type` (`"tachycardia"`, `"bradycardia"`, or `"normal"`)
   - Write these to Kafka topic `heart_rate_alerts` via `KafkaSink` (already stubbed).

The starter job currently just does a trivial `map` with a placeholder JSON payload. You must replace that with your real windowing logic.

**Important:** Do **not** simply copy Flink example code; adapt it thoughtfully. Use either:

- A built‑in windowed `aggregate` or `reduce` function + `ProcessWindowFunction`, or
- A `ProcessWindowFunction` alone that iterates over elements and computes stats.

Both are valid; choose what you understand.

### 2.4. Running the Flink job

From the repo root (with venv active and Flink cluster running):

```bash
cd ~/hw-kafka-flink-health
source venv/bin/activate

$FLINK_HOME/bin/flink run -py src/flink_job.py \
  --job_name "HeartRateAlertsJob"
```

If instructed by the instructor/TA (once the Kafka connector is installed under `/opt/flink/opt`), you may need to add a `--jarfile`:

```bash
$FLINK_HOME/bin/flink run \
  -py src/flink_job.py \
  --job_name "HeartRateAlertsJob" \
  --jarfile /opt/flink/opt/flink-connector-kafka-<version>.jar
```

Watch the Flink web UI (port 8081) to see your job running.

To **check alerts**:

```bash
$KAFKA_HOME/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic heart_rate_alerts \
  --from-beginning
```

You should see JSON strings with aggregated metrics and alert types.

---

## Part 3 (Optional): Spark Batch Analysis

File: `src/spark_batch_analysis.py` (optional; may be extra credit if defined by instructor)

Goal: Once your Flink job can also write alerts to **files** (e.g., JSON in `output/alerts/`), you can run a Spark batch job to analyze the alerts.

You’ll:

- Read alerts with Spark.
- Group by `patient_id` and `alert_type`.
- Compute the number of alerts per category.
- Write results to a new directory (e.g. in Parquet or CSV).

### Example run command

Assuming you modify `flink_job.py` (or write a small utility) to write alerts to `output/alerts/`:

```bash
$SPARK_HOME/bin/spark-submit \
  src/spark_batch_analysis.py \
  --input_path output/alerts \
  --output_path output/alert_stats
```

The script should:

- Use `SparkSession.read.json()` to load input data.
- Use simple DataFrame aggregations and `write.mode("overwrite")` to save results.

---

## Running the Unit Tests

Before submitting, run the tests:

```bash
cd ~/hw-kafka-flink-health
source venv/bin/activate
pytest -q
```

The provided tests will check:

- `generate_heart_rate_event` (basic structure & ranges).
- `parse_event` (valid input, invalid JSON, missing fields).
- `classify_window` thresholds.

**Note:** Passing the tests does **not** guarantee that your full streaming pipeline is correct, but it’s a strong signal that your core functions are implemented correctly.

---

## What to Submit

Unless instructed otherwise, you should submit:

- Your modified `src/flink_job.py`.
- Any changes to `src/kafka_producer.py`.
- (Optional) Your `src/spark_batch_analysis.py` if you attempted it.
- A brief `REPORT.md` or text note (if requested by instructor) explaining:
  - Window strategy (size/type).
  - Watermark/out‑of‑order delay settings.
  - How you tested your pipeline.
  - Any known limitations.

Do **not** include Kafka, Flink, or Spark binaries in your submission.

---

## Hints & Tips

- Start small: print out parsed events and verify timestamps / epoch millis first.
- Carefully think through **event time vs processing time**. We want event‑time windows.
- Keep your thresholds and logic consistent with the unit tests.
- Use Kafka console tools to debug issues at the boundaries (input and output topics).
- If your job “hangs” and windows don’t produce output, check:
  - Watermark configuration (maybe too large).
  - Whether you’re actually assigning timestamps correctly.
  - Whether your producer is sending events with timestamps in the near “present”.

Good luck, and have fun streaming!
