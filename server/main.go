// Command server runs the chat server (net/rpc, polling design).
//
// Clients call Join once, then Poll every few hundred ms. Each Poll returns
// and clears that client's pending events and doubles as a heartbeat: a
// client that stops polling is evicted by the reaper and a "leave" notice is
// broadcast, which handles crashes and dropped connections.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"

	"chatapp/chat"
)

const (
	maxQueue       = 1000 // per-client backlog; oldest events dropped beyond this
	sessionTimeout = 10 * time.Second
	reapInterval   = 2 * time.Second
	shutdownGrace  = 2 * time.Second
)

type session struct {
	token    string
	username string
	queue    []chat.Event
	lastSeen time.Time
}

// Service is the RPC receiver. All state is guarded by mu.
// Only the methods with the net/rpc signature are exported.
type Service struct {
	mu      sync.Mutex
	byToken map[string]*session
	byName  map[string]*session // lower-cased username -> session
	closing bool
}

func NewService() *Service {
	return &Service{
		byToken: make(map[string]*session),
		byName:  make(map[string]*session),
	}
}

func newToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// ---- RPC methods ----

func (s *Service) Join(args chat.JoinArgs, reply *chat.JoinReply) error {
	name := strings.TrimSpace(args.Username)
	if err := chat.ValidateUsername(name); err != nil {
		return err
	}
	token, err := newToken()
	if err != nil {
		return err
	}
	key := strings.ToLower(name)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return errors.New(chat.ErrServerClosing)
	}
	if _, taken := s.byName[key]; taken {
		return errors.New(chat.ErrUsernameTaken)
	}
	sess := &session{token: token, username: name, lastSeen: time.Now()}
	s.byToken[token] = sess
	s.byName[key] = sess
	s.broadcastLocked(chat.Event{Type: chat.EventJoin, From: name, Time: time.Now()}, sess)

	reply.Token = token
	reply.Users = s.usersLocked()
	log.Printf("join: %s (%d online)", name, len(s.byToken))
	return nil
}

func (s *Service) Leave(args chat.LeaveArgs, _ *chat.Empty) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.byToken[args.Token]
	if !ok {
		return nil // idempotent
	}
	s.removeLocked(sess, "")
	return nil
}

func (s *Service) Send(args chat.SendArgs, _ *chat.Empty) error {
	text := sanitize(args.Text)
	if text == "" {
		return errors.New(chat.ErrEmptyMessage)
	}
	if utf8.RuneCountInString(text) > chat.MaxMessageLen {
		return errors.New(chat.ErrMessageTooLong)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.byToken[args.Token]
	if !ok {
		return errors.New(chat.ErrInvalidSession)
	}
	if s.closing {
		return errors.New(chat.ErrServerClosing)
	}
	sess.lastSeen = time.Now()
	s.broadcastLocked(chat.Event{
		Type: chat.EventMessage, From: sess.username, Text: text, Time: time.Now(),
	}, sess) // sender excluded
	return nil
}

// Poll returns and clears the caller's pending events. It is also the heartbeat.
func (s *Service) Poll(args chat.PollArgs, reply *chat.PollReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.byToken[args.Token]
	if !ok {
		return errors.New(chat.ErrInvalidSession)
	}
	sess.lastSeen = time.Now()
	reply.Events = sess.queue
	sess.queue = nil
	return nil
}

func (s *Service) Users(args chat.UsersArgs, reply *chat.UsersReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.byToken[args.Token]
	if !ok {
		return errors.New(chat.ErrInvalidSession)
	}
	sess.lastSeen = time.Now()
	reply.Users = s.usersLocked()
	return nil
}

// ---- helpers (callers hold s.mu where the name ends in Locked) ----

// broadcastLocked queues ev for every session except skip (may be nil).
func (s *Service) broadcastLocked(ev chat.Event, skip *session) {
	for _, sess := range s.byToken {
		if sess == skip {
			continue
		}
		sess.queue = append(sess.queue, ev)
		if over := len(sess.queue) - maxQueue; over > 0 {
			sess.queue = append([]chat.Event(nil), sess.queue[over:]...)
		}
	}
}

func (s *Service) removeLocked(sess *session, detail string) {
	delete(s.byToken, sess.token)
	delete(s.byName, strings.ToLower(sess.username))
	s.broadcastLocked(chat.Event{
		Type: chat.EventLeave, From: sess.username, Text: detail, Time: time.Now(),
	}, nil)
	log.Printf("leave: %s %s(%d online)", sess.username, detailSuffix(detail), len(s.byToken))
}

func detailSuffix(d string) string {
	if d == "" {
		return ""
	}
	return "[" + d + "] "
}

func (s *Service) usersLocked() []string {
	names := make([]string, 0, len(s.byToken))
	for _, sess := range s.byToken {
		names = append(names, sess.username)
	}
	sort.Strings(names)
	return names
}

// reapOnce evicts sessions silent for longer than timeout.
func (s *Service) reapOnce(now time.Time, timeout time.Duration) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, sess := range s.byToken {
		if now.Sub(sess.lastSeen) > timeout {
			s.removeLocked(sess, "connection lost")
			n++
		}
	}
	return n
}

func (s *Service) reaper(ctx context.Context) {
	t := time.NewTicker(reapInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			s.reapOnce(now, sessionTimeout)
		}
	}
}

// shutdown refuses new work, tells everyone, and waits (up to grace) for
// clients to pick the notice up on their next poll.
func (s *Service) shutdown(grace time.Duration) {
	s.mu.Lock()
	s.closing = true
	s.broadcastLocked(chat.Event{
		Type: chat.EventShutdown, Text: chat.ErrServerClosing, Time: time.Now(),
	}, nil)
	s.mu.Unlock()

	deadline := time.Now().Add(grace)
	for time.Now().Before(deadline) {
		if s.drained() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (s *Service) drained() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sess := range s.byToken {
		if len(sess.queue) > 0 {
			return false
		}
	}
	return true
}

// sanitize drops control characters (blocks terminal escape injection)
// and trims surrounding whitespace.
func sanitize(text string) string {
	return strings.TrimSpace(strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, text))
}

// ---- connection tracking ----

type connSet struct {
	mu sync.Mutex
	m  map[net.Conn]struct{}
}

func (c *connSet) add(conn net.Conn) {
	c.mu.Lock()
	c.m[conn] = struct{}{}
	c.mu.Unlock()
}

func (c *connSet) remove(conn net.Conn) {
	c.mu.Lock()
	delete(c.m, conn)
	c.mu.Unlock()
}

func (c *connSet) closeAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for conn := range c.m {
		conn.Close()
	}
}

func main() {
	addr := flag.String("addr", ":1234", "listen address")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	svc := NewService()
	srv := rpc.NewServer()
	if err := srv.RegisterName(chat.ServiceName, svc); err != nil {
		log.Fatalf("register: %v", err)
	}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("chat server listening on %s (Ctrl+C to stop)", ln.Addr())

	var wg sync.WaitGroup
	conns := &connSet{m: make(map[net.Conn]struct{})}

	wg.Add(1)
	go func() {
		defer wg.Done()
		svc.reaper(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				log.Printf("accept: %v", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}
			conns.add(conn)
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conns.remove(conn)
				srv.ServeConn(conn) // one goroutine per connection; each call runs in its own goroutine
			}()
		}
	}()

	<-ctx.Done()
	log.Printf("shutting down: notifying clients")
	svc.shutdown(shutdownGrace)
	ln.Close()
	conns.closeAll()
	wg.Wait()
	log.Printf("bye")
}
