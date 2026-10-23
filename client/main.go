// Command client is the terminal chat client (net/rpc, polling design).
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"chatapp/chat"
)

const (
	pollInterval    = 500 * time.Millisecond
	callTimeout     = 3 * time.Second
	maxPollFailures = 3
)

var (
	errTimeout = errors.New("rpc call timed out")
	outMu      sync.Mutex // serialises terminal output
	skipLeave  atomic.Bool
)

// printLine prints s on its own line, then redraws the input prompt, so
// incoming events never garble what the user is typing on a fresh line.
func printLine(s string) {
	outMu.Lock()
	defer outMu.Unlock()
	fmt.Printf("\r\033[K%s\n> ", s)
}

// call invokes a Chat.<method> RPC with a timeout.
func call(c *rpc.Client, method string, args, reply any, d time.Duration) error {
	done := c.Go(chat.ServiceName+"."+method, args, reply, make(chan *rpc.Call, 1)).Done
	select {
	case r := <-done:
		return r.Error
	case <-time.After(d):
		return errTimeout
	}
}

func isServerError(err error) bool {
	var se rpc.ServerError
	return errors.As(err, &se)
}

func readLines() <-chan string {
	ch := make(chan string)
	go func() {
		defer close(ch)
		sc := bufio.NewScanner(os.Stdin)
		sc.Buffer(make([]byte, 64*1024), 1<<20)
		for sc.Scan() {
			ch <- sc.Text()
		}
	}()
	return ch
}

// join keeps asking for a username until the server accepts one.
func join(ctx context.Context, c *rpc.Client, name string, lines <-chan string) (string, chat.JoinReply, error) {
	for {
		if name == "" {
			fmt.Print("Choose a username: ")
			select {
			case <-ctx.Done():
				return "", chat.JoinReply{}, ctx.Err()
			case l, ok := <-lines:
				if !ok {
					return "", chat.JoinReply{}, io.EOF
				}
				name = strings.TrimSpace(l)
			}
		}
		if err := chat.ValidateUsername(name); err != nil {
			fmt.Println("Invalid username:", err)
			name = ""
			continue
		}
		var reply chat.JoinReply
		err := call(c, "Join", chat.JoinArgs{Username: name}, &reply, callTimeout)
		if err == nil {
			return name, reply, nil
		}
		if isServerError(err) {
			if err.Error() == chat.ErrServerClosing {
				return "", chat.JoinReply{}, err
			}
			fmt.Printf("Cannot join as %q: %v\n", name, err)
			name = ""
			continue
		}
		return "", chat.JoinReply{}, err
	}
}

func render(ev chat.Event) string {
	ts := ev.Time.Local().Format("15:04:05")
	switch ev.Type {
	case chat.EventMessage:
		return fmt.Sprintf("[%s] %s: %s", ts, ev.From, ev.Text)
	case chat.EventJoin:
		return fmt.Sprintf("[%s] *** %s joined the chat ***", ts, ev.From)
	case chat.EventLeave:
		if ev.Text != "" {
			return fmt.Sprintf("[%s] *** %s left the chat (%s) ***", ts, ev.From, ev.Text)
		}
		return fmt.Sprintf("[%s] *** %s left the chat ***", ts, ev.From)
	case chat.EventShutdown:
		return fmt.Sprintf("[%s] *** %s ***", ts, ev.Text)
	}
	return ""
}

// pollLoop fetches pending events and prints them as they arrive.
func pollLoop(ctx context.Context, cancel context.CancelFunc, c *rpc.Client, token string) {
	t := time.NewTicker(pollInterval)
	defer t.Stop()
	fails := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		var reply chat.PollReply
		err := call(c, "Poll", chat.PollArgs{Token: token}, &reply, callTimeout)
		if err != nil {
			if isServerError(err) { // e.g. evicted: session no longer valid
				skipLeave.Store(true)
				printLine("*** " + err.Error() + " ***")
				cancel()
				return
			}
			if fails++; fails >= maxPollFailures {
				skipLeave.Store(true)
				printLine("*** lost connection to server ***")
				cancel()
				return
			}
			continue
		}
		fails = 0
		for _, ev := range reply.Events {
			printLine(render(ev))
			if ev.Type == chat.EventShutdown {
				skipLeave.Store(true)
				cancel()
				return
			}
		}
	}
}

func run() int {
	addr := flag.String("addr", "localhost:1234", "server address")
	name := flag.String("name", "", "username (prompted if empty)")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	c, err := rpc.Dial("tcp", *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot connect to %s: %v\n", *addr, err)
		return 1
	}
	defer c.Close()

	lines := readLines()
	user, jr, err := join(ctx, c, *name, lines)
	if err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, io.EOF) {
			fmt.Fprintf(os.Stderr, "join failed: %v\n", err)
		}
		return 1
	}
	token := jr.Token

	go pollLoop(ctx, cancel, c, token)
	printLine(fmt.Sprintf("Joined as %s. Online: %s. Type /help for commands.",
		user, strings.Join(jr.Users, ", ")))

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case line, ok := <-lines:
			if !ok { // stdin closed
				break loop
			}
			line = strings.TrimSpace(line)
			switch {
			case line == "":
				printLine("")
			case line == "/quit":
				break loop
			case line == "/help":
				printLine("Commands: /users  list online users | /quit  leave | anything else is sent to everyone")
			case line == "/users":
				var ur chat.UsersReply
				if err := call(c, "Users", chat.UsersArgs{Token: token}, &ur, callTimeout); err != nil {
					printLine("error: " + err.Error())
				} else {
					printLine(fmt.Sprintf("Online (%d): %s", len(ur.Users), strings.Join(ur.Users, ", ")))
				}
			case strings.HasPrefix(line, "/"):
				printLine("unknown command (try /help)")
			default:
				if err := call(c, "Send", chat.SendArgs{Token: token, Text: line}, &chat.Empty{}, callTimeout); err != nil {
					printLine("send failed: " + err.Error())
					if err.Error() == chat.ErrInvalidSession {
						break loop
					}
				} else {
					printLine("")
				}
			}
		}
	}

	cancel()
	if !skipLeave.Load() {
		_ = call(c, "Leave", chat.LeaveArgs{Token: token}, &chat.Empty{}, 2*time.Second)
	}
	fmt.Println("\nbye")
	return 0
}

func main() { os.Exit(run()) }
