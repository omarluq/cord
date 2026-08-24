-- Immutable pre-lifecycle run fixtures.
--
-- These rows use the last public schema before lifecycle snapshot metadata.
-- Keep lifecycle columns out of this file: migrations must preserve history,
-- not manufacture it for terminal rows.
--
-- One-row defaults keep shared fixture values in one place. CTEs are used
-- instead of dialect-specific variables. Payloads use PostgreSQL's decode;
-- the SQLite test loader translates those calls to equivalent hex literals.
WITH
run_defaults (
    workflow_name, definition_hash, input_payload, terminal_node_id,
    created_at, terminal_at, max_attempts, retry_base_delay_ns,
    retry_max_delay_ns, retry_policy_version
) AS (VALUES (
    'legacy-terminal-public-get',
    '398d0d6840d16945034e347440543ecd5b78cab4dbe613b8a6d087e6e1a55db0',
    decode('3431', 'hex'),
    '56217dabec2e56581ce06a34e63d1752078b88820ee102888b2134ebd6fb4ccb',
    '2024-01-02T03:04:05Z', '2024-01-02T03:05:05Z',
    3, 500000000, 30000000000, 1
)),
run_rows (id, status, output_payload, error_payload, is_terminal) AS (VALUES
    ('legacy-active', 'running', NULL, NULL, 0),
    ('legacy-completed', 'completed', decode('3432', 'hex'), NULL, 1),
    ('legacy-failed', 'failed', NULL,
     decode('7B2274696D65223A22323032342D30312D30325430333A30353A30355A222C226D657373616765223A226C656761637920626F6F6D222C226E6F64655F6964223A2235363231376461626563326535363538316365303661333465363364313735323037386238383832306565313032383838623231333465626436666234636362222C2266756E6374696F6E5F6B6579223A226769746875622E636F6D2F6F6D61726C75712F636F72645F746573742E6164644F6E65222C22617474656D7074223A312C22726574727961626C65223A66616C73657D', 'hex'),
     1),
    ('legacy-canceled', 'canceled', NULL, NULL, 1)
)
INSERT INTO cord_runs (
    id, workflow_name, definition_hash, status, input_payload, output_payload,
    terminal_node_id, error_payload, created_at, updated_at, completed_at,
    max_attempts, retry_base_delay_ns, retry_max_delay_ns, retry_policy_version
)
SELECT
    rows.id, defaults.workflow_name, defaults.definition_hash, rows.status,
    defaults.input_payload, rows.output_payload, defaults.terminal_node_id,
    rows.error_payload, CAST(defaults.created_at AS TIMESTAMP),
    CAST(CASE WHEN rows.is_terminal = 1 THEN defaults.terminal_at ELSE defaults.created_at END AS TIMESTAMP),
    CAST(CASE WHEN rows.is_terminal = 1 THEN defaults.terminal_at END AS TIMESTAMP),
    defaults.max_attempts, defaults.retry_base_delay_ns,
    defaults.retry_max_delay_ns, defaults.retry_policy_version
FROM run_rows AS rows
CROSS JOIN run_defaults AS defaults;

WITH node_defaults (function_key, signature_hash, started_at, error_payload) AS (VALUES (
    'github.com/omarluq/cord_test.addOne',
    '7216fbe37502068fb755cfd1e3fa298b3f32f2ff34b860cb535332ad087afa65',
    '2024-01-02T03:04:35Z',
    decode('7B226D657373616765223A226C656761637920626F6F6D227D', 'hex')
))
INSERT INTO cord_nodes (
    run_id, node_id, function_key, signature_hash, status, remaining_deps,
    attempt, available_at, lease_owner, lease_generation, lease_expires_at,
    output_payload, error_payload, started_at, completed_at
)
SELECT
    runs.id, runs.terminal_node_id, defaults.function_key,
    defaults.signature_hash,
    CASE WHEN runs.completed_at IS NULL THEN 'ready' ELSE runs.status END,
    0,
    CASE WHEN runs.output_payload IS NOT NULL OR runs.error_payload IS NOT NULL THEN 1 ELSE 0 END,
    CAST(runs.created_at AS TIMESTAMP), NULL,
    CASE WHEN runs.output_payload IS NOT NULL OR runs.error_payload IS NOT NULL THEN 1 ELSE 0 END,
    NULL, runs.output_payload,
    CASE WHEN runs.error_payload IS NOT NULL THEN defaults.error_payload END,
    CASE
        WHEN runs.output_payload IS NOT NULL OR runs.error_payload IS NOT NULL
        THEN CAST(defaults.started_at AS TIMESTAMP)
    END,
    runs.completed_at
FROM cord_runs AS runs
CROSS JOIN node_defaults AS defaults;
