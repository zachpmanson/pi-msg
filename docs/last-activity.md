# Last Activity (XEP-0012)

pi-msg advertises `jabber:iq:last` through service discovery and entity
capabilities. An authorized XMPP client can query the bridge's **full JID**:

```xml
<iq type='get' from='you@example.com/phone' to='pi@example.com/pi-msg' id='last1'>
  <query xmlns='jabber:iq:last'/>
</iq>
```

The bridge replies to the requesting resource, preserving the request ID:

```xml
<iq type='result' to='you@example.com/phone' id='last1'>
  <query xmlns='jabber:iq:last' seconds='123'/>
</iq>
```

`seconds` is a nonnegative whole number of idle seconds, using the same clock
as XEP-0319 presence: the agent becomes idle when its work settles and active
when it receives activity or starts work. Active agents report `0`. This is
**agent activity**, not a human's last keyboard interaction, process uptime,
or time since the last network stanza. Status text, prompts and tasks are
never included. IQ queries themselves do not wake the agent or reset its clock.

## Privacy

[XEP-0012 §§4,8](https://xmpp.org/extensions/xep-0012.html#online) requires the
**XMPP server** to reject requests from entities not authorized to view the
account's presence with `auth/forbidden`, before forwarding any full-JID query.
The server must also enforce privacy lists and blocking. pi-msg relies on that
server-side authorization; its message allowlist, `allowArbitraryJid`, and MUC
occupant tracking are **not** presence subscriptions and do not grant access.
Use a server that enforces these XEP-0012 checks.

Set `"lastActivity": false` on the account to disable these responses. The
bridge then returns `cancel/service-unavailable` without a `seconds` attribute.
Discovery still advertises protocol support, not permission to receive data.
This switch affects XEP-0012 only; existing XEP-0319 idle presence is unchanged.

Queries to the account's **bare JID** (offline last-seen and last unavailable
status, or `0` while online) and to a **domain JID** (server uptime) are handled
by the server, not pi-msg. Their privacy settings are managed on the server.
This feature adds the resource responder, not an agent tool for querying peers.
