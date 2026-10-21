-- Jobs are the single source of truth. Kafka only carries "please look at job X" signals;
-- every state transition is a conditional UPDATE against this table.
CREATE TABLE jobs (
    id               UUID PRIMARY KEY,
    type             VARCHAR(100)  NOT NULL,
    payload          JSONB         NOT NULL DEFAULT '{}'::jsonb,
    status           VARCHAR(20)   NOT NULL,
    priority         INT           NOT NULL DEFAULT 0,
    idempotency_key  VARCHAR(200)  UNIQUE,

    attempt          INT           NOT NULL DEFAULT 0,
    max_attempts     INT           NOT NULL DEFAULT 5,
    next_run_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    queued_at        TIMESTAMPTZ,

    -- Lease: which worker owns the job and until when. `fence` increments on every claim,
    -- so a worker that lost its lease can never overwrite the state written by its successor.
    lease_owner      VARCHAR(100),
    lease_expires_at TIMESTAMPTZ,
    fence            BIGINT        NOT NULL DEFAULT 0,
    cancel_requested BOOLEAN       NOT NULL DEFAULT false,

    checkpoint       JSONB,
    progress         INT           NOT NULL DEFAULT 0,
    result           JSONB,
    last_error       TEXT,

    created_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),

    CONSTRAINT jobs_status_chk CHECK (status IN
        ('SCHEDULED', 'QUEUED', 'RUNNING', 'SUCCEEDED', 'DEAD', 'CANCELLED'))
);

-- Dispatcher scans due jobs; sweeper scans expired leases and stuck queued jobs.
CREATE INDEX jobs_due_idx      ON jobs (next_run_at, priority DESC) WHERE status = 'SCHEDULED';
CREATE INDEX jobs_lease_idx    ON jobs (lease_expires_at)          WHERE status = 'RUNNING';
CREATE INDEX jobs_queued_idx   ON jobs (queued_at)                 WHERE status = 'QUEUED';
CREATE INDEX jobs_status_idx   ON jobs (status, created_at DESC);

CREATE TABLE job_events (
    id         BIGSERIAL PRIMARY KEY,
    job_id     UUID         NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    type       VARCHAR(40)  NOT NULL,
    attempt    INT          NOT NULL,
    worker     VARCHAR(100),
    message    TEXT,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX job_events_job_idx ON job_events (job_id, id);

-- Used by the Postgres coordination backend (leader election + worker registry)
-- when Redis is not configured.
CREATE TABLE leader_leases (
    name       VARCHAR(100) PRIMARY KEY,
    owner      VARCHAR(100) NOT NULL,
    expires_at TIMESTAMPTZ  NOT NULL
);

CREATE TABLE workers (
    id             VARCHAR(100) PRIMARY KEY,
    host           VARCHAR(200),
    capacity       INT          NOT NULL,
    active_jobs    INT          NOT NULL,
    started_at     TIMESTAMPTZ  NOT NULL,
    last_heartbeat TIMESTAMPTZ  NOT NULL
);
