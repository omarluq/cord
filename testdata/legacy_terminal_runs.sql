-- Immutable pre-lifecycle run fixtures.
--
-- These rows use the last public schema before lifecycle snapshot metadata.
-- Keep lifecycle columns out of this file: migrations must preserve history,
-- not manufacture it for terminal rows.
INSERT INTO cord_runs (
    id, workflow_name, definition_hash, status, input_payload, output_payload,
    terminal_node_id, error_payload, created_at, updated_at, completed_at,
    max_attempts, retry_base_delay_ns, retry_max_delay_ns, retry_policy_version
) VALUES
    ('legacy-active', 'legacy-terminal-public-get',
     '398d0d6840d16945034e347440543ecd5b78cab4dbe613b8a6d087e6e1a55db0',
     'running', X'3431', NULL,
     '56217dabec2e56581ce06a34e63d1752078b88820ee102888b2134ebd6fb4ccb', NULL,
     '2024-01-02T03:04:05Z', '2024-01-02T03:04:05Z', NULL,
     3, 500000000, 30000000000, 1),
    ('legacy-completed', 'legacy-terminal-public-get',
     '398d0d6840d16945034e347440543ecd5b78cab4dbe613b8a6d087e6e1a55db0',
     'completed', X'3431', X'3432',
     '56217dabec2e56581ce06a34e63d1752078b88820ee102888b2134ebd6fb4ccb', NULL,
     '2024-01-02T03:04:05Z', '2024-01-02T03:05:05Z', '2024-01-02T03:05:05Z',
     3, 500000000, 30000000000, 1),
    ('legacy-failed', 'legacy-terminal-public-get',
     '398d0d6840d16945034e347440543ecd5b78cab4dbe613b8a6d087e6e1a55db0',
     'failed', X'3431', NULL,
     '56217dabec2e56581ce06a34e63d1752078b88820ee102888b2134ebd6fb4ccb',
     X'7B2274696D65223A22323032342D30312D30325430333A30353A30355A222C226D657373616765223A226C656761637920626F6F6D222C226E6F64655F6964223A2235363231376461626563326535363538316365303661333465363364313735323037386238383832306565313032383838623231333465626436666234636362222C2266756E6374696F6E5F6B6579223A226769746875622E636F6D2F6F6D61726C75712F636F72645F746573742E6164644F6E65222C22617474656D7074223A312C22726574727961626C65223A66616C73657D',
     '2024-01-02T03:04:05Z', '2024-01-02T03:05:05Z', '2024-01-02T03:05:05Z',
     3, 500000000, 30000000000, 1),
    ('legacy-canceled', 'legacy-terminal-public-get',
     '398d0d6840d16945034e347440543ecd5b78cab4dbe613b8a6d087e6e1a55db0',
     'canceled', X'3431', NULL,
     '56217dabec2e56581ce06a34e63d1752078b88820ee102888b2134ebd6fb4ccb', NULL,
     '2024-01-02T03:04:05Z', '2024-01-02T03:05:05Z', '2024-01-02T03:05:05Z',
     3, 500000000, 30000000000, 1);

INSERT INTO cord_nodes (
    run_id, node_id, function_key, signature_hash, status, remaining_deps,
    attempt, available_at, lease_owner, lease_generation, lease_expires_at,
    output_payload, error_payload, started_at, completed_at
) VALUES
    ('legacy-active', '56217dabec2e56581ce06a34e63d1752078b88820ee102888b2134ebd6fb4ccb',
     'github.com/omarluq/cord_test.addOne',
     '7216fbe37502068fb755cfd1e3fa298b3f32f2ff34b860cb535332ad087afa65',
     'ready', 0, 0, '2024-01-02T03:04:05Z', NULL, 0, NULL,
     NULL, NULL, NULL, NULL),
    ('legacy-completed', '56217dabec2e56581ce06a34e63d1752078b88820ee102888b2134ebd6fb4ccb',
     'github.com/omarluq/cord_test.addOne',
     '7216fbe37502068fb755cfd1e3fa298b3f32f2ff34b860cb535332ad087afa65',
     'completed', 0, 1, '2024-01-02T03:04:05Z', NULL, 1, NULL,
     X'3432', NULL, '2024-01-02T03:04:35Z', '2024-01-02T03:05:05Z'),
    ('legacy-failed', '56217dabec2e56581ce06a34e63d1752078b88820ee102888b2134ebd6fb4ccb',
     'github.com/omarluq/cord_test.addOne',
     '7216fbe37502068fb755cfd1e3fa298b3f32f2ff34b860cb535332ad087afa65',
     'failed', 0, 1, '2024-01-02T03:04:05Z', NULL, 1, NULL,
     NULL, X'7B226D657373616765223A226C656761637920626F6F6D227D',
     '2024-01-02T03:04:35Z', '2024-01-02T03:05:05Z'),
    ('legacy-canceled', '56217dabec2e56581ce06a34e63d1752078b88820ee102888b2134ebd6fb4ccb',
     'github.com/omarluq/cord_test.addOne',
     '7216fbe37502068fb755cfd1e3fa298b3f32f2ff34b860cb535332ad087afa65',
     'canceled', 0, 0, '2024-01-02T03:04:05Z', NULL, 0, NULL,
     NULL, NULL, NULL, '2024-01-02T03:05:05Z');
