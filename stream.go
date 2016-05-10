package eventsource

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

// Stream handles a connection for receiving Server Sent Events.
// It will try and reconnect if the connection is lost, respecting both
// received retry delays and event id's.
type Stream struct {
	c           http.Client
	url         string
	lastEventId string
	retry       time.Duration
	stopCh      chan struct{}
	// Events emits the events received by the stream
	Events chan Event
	// Errors emits any errors encountered while reading events from the stream.
	// It's mainly for informative purposes - the client isn't required to take any
	// action when an error is encountered. The stream will always attempt to continue,
	// even if that involves reconnecting to the server.
	Errors chan error
}

type SubscriptionError struct {
	Code    int
	Message string
}

func (e SubscriptionError) Error() string {
	return fmt.Sprintf("%d: %s", e.Code, e.Message)
}

// Subscribe to the Events emitted from the specified url.
// If lastEventId is non-empty it will be sent to the server in case it can replay missed events.
func Subscribe(url, lastEventId string, tr *http.Transport) (*Stream, error) {
	stream := &Stream{
		url:         url,
		lastEventId: lastEventId,
		stopCh:      make(chan struct{}),
		retry:       (time.Millisecond * 1000),
		Events:      make(chan Event),
		Errors:      make(chan error),
	}
	// Optional custom HTTP transport for more flexible configuration.
	if tr != nil {
		stream.c = http.Client{Transport: tr}
	}
	r, err := stream.connect()
	if err != nil {
		return nil, err
	}
	go stream.cleanupOnStop()
	go stream.stream(r)
	return stream, nil
}

func (stream *Stream) cleanupOnStop() {
	<-stream.stopCh
	close(stream.Errors)
	close(stream.Events)
}

func (stream *Stream) Stop() {
	close(stream.stopCh)
}

func (stream *Stream) connect() (r io.ReadCloser, err error) {
	var resp *http.Response
	var req *http.Request
	if req, err = http.NewRequest("GET", stream.url, nil); err != nil {
		return
	}
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Accept", "text/event-stream")
	if len(stream.lastEventId) > 0 {
		req.Header.Set("Last-Event-ID", stream.lastEventId)
	}
	if resp, err = stream.c.Do(req); err != nil {
		return
	}
	if resp.StatusCode != 200 {
		message, _ := ioutil.ReadAll(resp.Body)
		err = SubscriptionError{
			Code:    resp.StatusCode,
			Message: string(message),
		}
	}
	r = resp.Body
	return
}

func (stream *Stream) stream(r io.ReadCloser) {
	defer r.Close()
	dec := newDecoder(r)
Stream:
	for {
		evCh := make(chan Event)
		errCh := make(chan error)

		go func(evCh chan<- Event, errCh chan<- error) {
			ev, err := dec.Decode()
			if err != nil {
				errCh <- err
				return
			}
			evCh <- ev
		}(evCh, errCh)

		select {
		case ev := <-evCh:
			pub := ev.(*publication)
			if pub.Retry() > 0 {
				stream.retry = time.Duration(pub.Retry()) * time.Millisecond
			}
			if len(pub.Id()) > 0 {
				stream.lastEventId = pub.Id()
			}
			stream.Events <- ev
		case err := <-errCh:
			stream.Errors <- err
			// respond to all errors by reconnecting and trying again
			break Stream
		case <-stream.stopCh:
			return
		}
	}
	backoff := stream.retry
	for {
		select {
		case <-time.After(backoff):
			break
		case <-stream.stopCh:
			return
		}

		// NOTE: because of the defer we're opening the new connection
		// before closing the old one. Shouldn't be a problem in practice,
		// but something to be aware of.
		next, err := stream.connect()
		if err == nil {
			go stream.stream(next)
			break
		}
		stream.Errors <- err

		// don't let the exponential backoff go to over 64 seconds...
		if backoff < (time.Millisecond * 64000) {
			backoff *= 2
		}
	}
}
