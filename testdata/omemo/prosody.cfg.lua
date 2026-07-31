-- Minimal Prosody for the OMEMO live integration test (docs/omemo.md).
-- PEP + pubsub enabled so bundles/devicelists can be published and fetched.
-- Not for production: plaintext auth is allowed to keep local setup trivial.
pidfile = "/tmp/prosody.pid"
admins = { }
modules_enabled = {
  "roster"; "saslauth"; "tls"; "dialback"; "disco";
  "private"; "vcard4"; "vcard_legacy";
  "ping"; "pep"; "carbons"; "pubsub";
}
allow_registration = false
c2s_require_encryption = false
allow_unencrypted_plain_auth = true
authentication = "internal_plain"
log = { info = "*console" }

VirtualHost "localhost"
  ssl = {
    key = "/etc/prosody/certs/localhost.key";
    certificate = "/etc/prosody/certs/localhost.crt";
  }
