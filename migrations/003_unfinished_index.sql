-- Serves the dispatcher's "oldest unfinished request" gauge (pipeline_oldest_unfinished_seconds) cheaply.
CREATE INDEX requests_unfinished_created ON requests (created_at)
    WHERE status IN ('received', 'processing', 'retry_scheduled');
