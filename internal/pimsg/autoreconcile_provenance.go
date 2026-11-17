package pimsg

// autoRoomOrigin reuses ONLY retained, admitted original delivery / accepted
// outbound provenance. No extra journal is created, and archive reads cannot
// establish proof. A room nickname (including today's occupant mapping) cannot
// establish historical identity. Old/out-of-retention entries remain unknown.
func (b *Bridge) autoRoomOrigin(room, from, id, body string) string {
	if b.acct.AutoreconcileTimeout <= 0 || id == "" {
		return ""
	}
	b.auto.mu.Lock()
	defer b.auto.mu.Unlock()
	for _, window := range [][]autoChat{b.auto.state.History.Recent, b.auto.state.History.Owner} {
		for _, e := range window {
			if e.Kind != "room" || e.Conversation != room || e.ID != id || e.Body != autoText(body, autoBodyBytes) {
				continue
			}
			if e.Direction == "out" && e.Sender == bareJid(b.acct.JID) {
				return e.Sender
			}
			if e.Direction == "in" && e.OriginFrom != "" && e.OriginFrom == from {
				return e.Sender
			}
		}
	}
	return ""
}
