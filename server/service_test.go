package main

import (
	"strings"
	"sync"
	"testing"
	"time"

	"chatapp/chat"
)

func mustJoin(t *testing.T, s *Service, name string) string {
	t.Helper()
	var r chat.JoinReply
	if err := s.Join(chat.JoinArgs{Username: name}, &r); err != nil {
		t.Fatalf("join %q: %v", name, err)
	}
	return r.Token
}

func mustPoll(t *testing.T, s *Service, token string) []chat.Event {
	t.Helper()
	var r chat.PollReply
	if err := s.Poll(chat.PollArgs{Token: token}, &r); err != nil {
		t.Fatalf("poll: %v", err)
	}
	return r.Events
}

func mustSend(t *testing.T, s *Service, token, text string) {
	t.Helper()
	if err := s.Send(chat.SendArgs{Token: token, Text: text}, &chat.Empty{}); err != nil {
		t.Fatalf("send: %v", err)
	}
}

func TestDuplicateUsername(t *testing.T) {
	s := NewService()
	mustJoin(t, s, "Alice")
	var r chat.JoinReply
	err := s.Join(chat.JoinArgs{Username: "alice"}, &r) // case-insensitive
	if err == nil || err.Error() != chat.ErrUsernameTaken {
		t.Fatalf("want %q, got %v", chat.ErrUsernameTaken, err)
	}
}

func TestInvalidUsername(t *testing.T) {
	s := NewService()
	for _, name := range []string{"", "   ", "has space", strings.Repeat("x", 21), "bad\x1b[31m"} {
		var r chat.JoinReply
		if err := s.Join(chat.JoinArgs{Username: name}, &r); err == nil {
			t.Errorf("username %q accepted", name)
		}
	}
}

func TestBroadcastExcludesSender(t *testing.T) {
	s := NewService()
	a := mustJoin(t, s, "alice")
	b := mustJoin(t, s, "bob")
	c := mustJoin(t, s, "carol")

	mustSend(t, s, a, "hello")

	for name, tok := range map[string]string{"bob": b, "carol": c} {
		var got []string
		for _, ev := range mustPoll(t, s, tok) {
			if ev.Type == chat.EventMessage {
				got = append(got, ev.From+":"+ev.Text)
			}
		}
		if len(got) != 1 || got[0] != "alice:hello" {
			t.Errorf("%s got %v", name, got)
		}
	}
	for _, ev := range mustPoll(t, s, a) {
		if ev.Type == chat.EventMessage {
			t.Errorf("sender received own message: %+v", ev)
		}
	}
}

func TestJoinAndLeaveNotifications(t *testing.T) {
	s := NewService()
	a := mustJoin(t, s, "alice")
	b := mustJoin(t, s, "bob")

	evs := mustPoll(t, s, a)
	if len(evs) != 1 || evs[0].Type != chat.EventJoin || evs[0].From != "bob" {
		t.Fatalf("alice expected join(bob), got %+v", evs)
	}
	if got := mustPoll(t, s, b); len(got) != 0 {
		t.Fatalf("joiner should not be notified of own join: %+v", got)
	}

	if err := s.Leave(chat.LeaveArgs{Token: b}, &chat.Empty{}); err != nil {
		t.Fatal(err)
	}
	evs = mustPoll(t, s, a)
	if len(evs) != 1 || evs[0].Type != chat.EventLeave || evs[0].From != "bob" {
		t.Fatalf("alice expected leave(bob), got %+v", evs)
	}
	// Name is reusable after leaving; Leave is idempotent.
	mustJoin(t, s, "bob")
	if err := s.Leave(chat.LeaveArgs{Token: b}, &chat.Empty{}); err != nil {
		t.Fatal(err)
	}
}

func TestInvalidSession(t *testing.T) {
	s := NewService()
	var pr chat.PollReply
	if err := s.Poll(chat.PollArgs{Token: "nope"}, &pr); err == nil || err.Error() != chat.ErrInvalidSession {
		t.Fatalf("poll: %v", err)
	}
	if err := s.Send(chat.SendArgs{Token: "nope", Text: "x"}, &chat.Empty{}); err == nil {
		t.Fatal("send with bad token accepted")
	}
}

func TestMessageValidationAndSanitizing(t *testing.T) {
	s := NewService()
	a := mustJoin(t, s, "alice")
	b := mustJoin(t, s, "bob")
	mustPoll(t, s, a)

	if err := s.Send(chat.SendArgs{Token: a, Text: "  \t "}, &chat.Empty{}); err == nil {
		t.Error("blank message accepted")
	}
	if err := s.Send(chat.SendArgs{Token: a, Text: strings.Repeat("x", chat.MaxMessageLen+1)}, &chat.Empty{}); err == nil {
		t.Error("oversized message accepted")
	}
	mustSend(t, s, a, "hi\x1b[2Jthere")
	evs := mustPoll(t, s, b)
	if len(evs) != 1 || evs[0].Text != "hi[2Jthere" {
		t.Fatalf("control chars not stripped: %+v", evs)
	}
}

func TestReapEvictsSilentClients(t *testing.T) {
	s := NewService()
	a := mustJoin(t, s, "alice")
	mustJoin(t, s, "ghost") // never polls again

	time.Sleep(20 * time.Millisecond)
	mustPoll(t, s, a) // alice stays alive
	n := s.reapOnce(time.Now().Add(5*time.Millisecond), 15*time.Millisecond)
	// ghost is older than the timeout, alice was refreshed just now
	if n != 1 {
		t.Fatalf("reaped %d, want 1", n)
	}
	var left bool
	for _, ev := range mustPoll(t, s, a) {
		if ev.Type == chat.EventLeave && ev.From == "ghost" && ev.Text != "" {
			left = true
		}
	}
	if !left {
		t.Fatal("no leave notice for evicted client")
	}
}

func TestShutdownNotifiesAndRefusesNewWork(t *testing.T) {
	s := NewService()
	a := mustJoin(t, s, "alice")

	done := make(chan struct{})
	go func() { s.shutdown(2 * time.Second); close(done) }()

	var sawShutdown bool
	deadline := time.Now().Add(time.Second)
	for !sawShutdown && time.Now().Before(deadline) {
		for _, ev := range mustPoll(t, s, a) {
			if ev.Type == chat.EventShutdown {
				sawShutdown = true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !sawShutdown {
		t.Fatal("no shutdown event")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not return after clients drained")
	}
	var r chat.JoinReply
	if err := s.Join(chat.JoinArgs{Username: "late"}, &r); err == nil {
		t.Fatal("join accepted during shutdown")
	}
}

// Run with -race: many clients joining, sending, polling at once.
func TestConcurrentClients(t *testing.T) {
	const n, m = 20, 50
	s := NewService()

	names := make([]string, n)
	tokens := make([]string, n)
	got := make([]int, n)
	for i := range names {
		names[i] = "user" + string(rune('A'+i))
	}

	var joined, done sync.WaitGroup
	joined.Add(n)
	done.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer done.Done()
			var jr chat.JoinReply
			if err := s.Join(chat.JoinArgs{Username: names[i]}, &jr); err != nil {
				t.Errorf("join: %v", err)
				joined.Done()
				return
			}
			tokens[i] = jr.Token
			joined.Done()
			joined.Wait() // everyone is in before anyone talks

			count := func() {
				var pr chat.PollReply
				if err := s.Poll(chat.PollArgs{Token: jr.Token}, &pr); err != nil {
					t.Errorf("poll: %v", err)
					return
				}
				for _, ev := range pr.Events {
					if ev.Type == chat.EventMessage {
						if ev.From == names[i] {
							t.Errorf("%s received own message", names[i])
						}
						got[i]++
					}
				}
			}
			for k := 0; k < m; k++ {
				if err := s.Send(chat.SendArgs{Token: jr.Token, Text: "msg"}, &chat.Empty{}); err != nil {
					t.Errorf("send: %v", err)
					return
				}
				count()
			}
		}(i)
	}
	done.Wait()

	for i := 0; i < n; i++ { // final drain
		for _, ev := range mustPoll(t, s, tokens[i]) {
			if ev.Type == chat.EventMessage {
				got[i]++
			}
		}
		if want := (n - 1) * m; got[i] != want {
			t.Errorf("%s received %d messages, want %d", names[i], got[i], want)
		}
	}
}
