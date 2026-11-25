-- Demo tenants for local runs and tests. Keys are test values (see README "Try it"):
--   tenant-a      demo-key-a      webhook to the mock's sink, generous limits
--   tenant-b      demo-key-b      no webhook: polls for results
--   tenant-noisy  demo-key-noisy  tight rate limit, used to show 429 isolation
INSERT INTO tenants (id, api_key_hash, webhook_url, webhook_secret, rate_per_sec, burst) VALUES
  ('tenant-a',     'f15424e984f66fb993b9d8e6444ade02adcc714a58b3b50106e98750e4244bfe',
                   'http://downstream-mock:8081/webhooks/tenant-a', 'demo-secret-a', 2000, 4000),
  ('tenant-b',     'd9ac7631ada741dcfc4b59392980617b80ca853433a835ca1baa8a484a35e893', NULL, NULL, 2000, 4000),
  ('tenant-noisy', 'a8c08b6d4efc802e2409814af20adaf6d220da5a1b22ce9074f6ef43c68cd5a3', NULL, NULL, 5, 5);
