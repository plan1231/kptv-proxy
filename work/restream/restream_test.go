package restream

import (
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bbuffer "kptv-proxy/work/buffer"
	"kptv-proxy/work/constants"
	"kptv-proxy/work/types"

	"github.com/puzpuzpuz/xsync/v3"
)

type testResponseWriter struct {
	header http.Header
	err    error
	writes atomic.Int64
}

func newTestResponseWriter() *testResponseWriter {
	return &testResponseWriter{header: make(http.Header)}
}

func (w *testResponseWriter) Header() http.Header {
	return w.header
}

func (w *testResponseWriter) Write(data []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	w.writes.Add(1)
	return len(data), nil
}

func (w *testResponseWriter) WriteHeader(statusCode int) {}

func (w *testResponseWriter) Flush() {}

func (w *testResponseWriter) SetWriteDeadline(deadline time.Time) error {
	return nil
}

func newTestRestream(t *testing.T) *Restream {
	t.Helper()

	return &Restream{Restreamer: &types.Restreamer{
		Channel: &types.Channel{
			Name: "test-channel",
		},
		Clients: xsync.NewMapOf[string, *types.RestreamClient](),
		Buffer:  bbuffer.NewRingBuffer(1024 * 1024),
	}}
}

func addTestClient(r *Restream, id string, writer *testResponseWriter, startWriter bool, queueDepth int) *types.RestreamClient {
	client := &types.RestreamClient{
		Id:      id,
		Writer:  writer,
		Flusher: writer,
		Done:    make(chan bool),
		Queue:   make(chan []byte, queueDepth),
	}
	client.LastSeen.Store(time.Now().Unix())
	r.Clients.Store(id, client)
	if startWriter {
		go r.writeClientLoop(client)
	}
	return client
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}

func TestDistributeToClientsFastClientContinuesWhenSlowQueueStopsDraining(t *testing.T) {
	r := newTestRestream(t)
	fastWriter := newTestResponseWriter()
	slowWriter := newTestResponseWriter()

	fastClient := addTestClient(r, "fast", fastWriter, false, 8)
	addTestClient(r, "slow", slowWriter, false, 1)

	chunk := []byte("chunk")
	for i := 0; i < 3; i++ {
		r.DistributeToClients(chunk)
	}

	if got := len(fastClient.Queue); got != 3 {
		t.Fatalf("fast client queued chunks = %d, want 3", got)
	}

	if _, fastExists := r.Clients.Load("fast"); !fastExists {
		t.Fatal("fast client was removed when slow client fell behind")
	}
	if _, slowExists := r.Clients.Load("slow"); slowExists {
		t.Fatal("slow client still exists after its queue stopped draining")
	}
}

func TestDistributeToClientsRemovesClientWhenQueueIsFull(t *testing.T) {
	r := newTestRestream(t)
	writer := newTestResponseWriter()

	addTestClient(r, "slow", writer, false, 1)

	if active := r.DistributeToClients([]byte("first")); active != 1 {
		t.Fatalf("first distribution active clients = %d, want 1", active)
	}
	if active := r.DistributeToClients([]byte("second")); active != 0 {
		t.Fatalf("second distribution active clients = %d, want 0", active)
	}

	if _, exists := r.Clients.Load("slow"); exists {
		t.Fatal("slow client still exists after queue overflow")
	}
}

func TestWriteErrorRemovesOnlyFailedClient(t *testing.T) {
	r := newTestRestream(t)
	fastWriter := newTestResponseWriter()
	failingWriter := newTestResponseWriter()
	failingWriter.err = errors.New("write failed")

	addTestClient(r, "fast", fastWriter, true, 4)
	addTestClient(r, "failing", failingWriter, true, 4)

	r.DistributeToClients([]byte("chunk"))

	waitFor(t, func() bool {
		_, failingExists := r.Clients.Load("failing")
		return fastWriter.writes.Load() == 1 && !failingExists
	})

	if _, fastExists := r.Clients.Load("fast"); !fastExists {
		t.Fatal("fast client was removed after another client write failed")
	}
}

func TestRemoveClientIsIdempotent(t *testing.T) {
	r := newTestRestream(t)
	writer := newTestResponseWriter()
	client := addTestClient(r, "client", writer, false, 1)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.RemoveClient("client")
		}()
	}
	wg.Wait()

	select {
	case <-client.Done:
	case <-time.After(time.Second):
		t.Fatal("client Done channel was not closed")
	}

	if _, exists := r.Clients.Load("client"); exists {
		t.Fatal("client still exists after repeated removal")
	}
}

func TestClientWriteQueueDepthMatchesBacklogBudget(t *testing.T) {
	wantBytes := constants.Internal.ClientBacklogBitrateBps * constants.Internal.ClientBacklogBudget.Nanoseconds() / int64(8*time.Second)
	wantDepth := int((wantBytes + int64(constants.Internal.StreamBufferSize) - 1) / int64(constants.Internal.StreamBufferSize))

	if got := clientWriteQueueDepth(); got != wantDepth {
		t.Fatalf("clientWriteQueueDepth() = %d, want %d", got, wantDepth)
	}
}
