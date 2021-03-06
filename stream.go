package eventsource

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

// Stream handles a connection for receiving Server Sent Events.
// It will try and reconnect if the connection is lost, respecting both
// received retry delays and event id's.
type Stream struct {
	c           *http.Client
	req         *http.Request
	lastEventID string
	retry       time.Duration
	stopCh      chan struct{}
	// Events emits the events received by the stream
	Events chan Event
	// Errors emits any errors encountered while reading events from the stream.
	// It's mainly for informative purposes - the client isn't required to take any
	// action when an error is encountered. The stream will always attempt to continue,
	// even if that involves reconnecting to the server.
	Errors chan error
	// Logger is a logger that, when set, will be used for logging debug messages
	Logger *log.Logger
}

type SubscriptionError struct {
	Code    int
	Message string
}

func (e SubscriptionError) Error() string {
	return fmt.Sprintf("%d: %s", e.Code, e.Message)
}

// Subscribe to the Events emitted from the specified url.
// If lastEventID is non-empty it will be sent to the server in case it can replay missed events.
func Subscribe(url, lastEventID string) (*Stream, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	return SubscribeWithRequest(lastEventID, req)
}

// SubscribeWithRequest will take an http.Request to setup the stream, allowing custom headers
// to be specified, authentication to be configured, etc.
func SubscribeWithRequest(lastEventID string, request *http.Request) (*Stream, error) {
	return SubscribeWith(lastEventID, http.DefaultClient, request)
}

// SubscribeWith takes a http client and request providing customization over both headers and
// control over the http client settings (timeouts, tls, etc)
func SubscribeWith(lastEventID string, client *http.Client, request *http.Request) (*Stream, error) {
	stream := &Stream{
		c:           client,
		req:         request,
		lastEventID: lastEventID,
		stopCh:      make(chan struct{}),
		retry:       (time.Millisecond * 1000),
		Events:      make(chan Event),
		Errors:      make(chan error),
	}
	stream.c.CheckRedirect = checkRedirect

	r, err := stream.connect()
	if err != nil {
		return nil, err
	}
	go stream.stream(r)
	return stream, nil
}

func (stream *Stream) Stop() {
	close(stream.stopCh)
}

// Go's http package doesn't copy headers across when it encounters
// redirects so we need to do that manually.
func checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return errors.New("stopped after 10 redirects")
	}
	for k, vv := range via[0].Header {
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}
	return nil
}

func (stream *Stream) connect() (r io.ReadCloser, err error) {
	var resp *http.Response
	stream.req.Header.Set("Cache-Control", "no-cache")
	stream.req.Header.Set("Accept", "text/event-stream")
	if len(stream.lastEventID) > 0 {
		stream.req.Header.Set("Last-Event-ID", stream.lastEventID)
	}
	if resp, err = stream.c.Do(stream.req); err != nil {
		return
	}
	if resp.StatusCode != 200 {
		message, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		err = SubscriptionError{
			Code:    resp.StatusCode,
			Message: string(message),
		}
		return
	}
	r = resp.Body
	return
}

func (stream *Stream) stream(r io.ReadCloser) {
	defer close(stream.Errors)
	defer close(stream.Events)

	for {
		err := stream.readFromStream(r)
		if err == nil {
			return
		}
		r = stream.reconnectWithBackoff()
		if r == nil {
			return
		}
	}
}

func (stream *Stream) readFromStream(r io.ReadCloser) error {
	defer r.Close()

	dec := NewDecoder(r)
	for {
		evCh := make(chan Event, 1)
		errCh := make(chan error, 1)

		go func() {
			ev, err := dec.Decode()
			if err != nil {
				errCh <- err
				return
			}
			evCh <- ev
		}()

		select {
		case ev := <-evCh:
			pub := ev.(*publication)
			if pub.Retry() > 0 {
				stream.retry = time.Duration(pub.Retry()) * time.Millisecond
			}
			if len(pub.Id()) > 0 {
				stream.lastEventID = pub.Id()
			}
			select {
			case stream.Events <- ev:
			case <-stream.stopCh:
			}
		case err := <-errCh:
			select {
			case stream.Errors <- err:
			case <-stream.stopCh:
			}
			// respond to all errors by reconnecting and trying again
			return err
		case <-stream.stopCh:
			return nil
		}
	}
}

func (stream *Stream) reconnectWithBackoff() io.ReadCloser {
	backoff := stream.retry
	for {
		select {
		case <-time.After(backoff):
			break
		case <-stream.stopCh:
			return nil
		}
		if stream.Logger != nil {
			stream.Logger.Printf("Reconnecting in %0.4f secs\n", backoff.Seconds())
		}

		next, err := stream.connect()
		if err == nil {
			return next
		}
		stream.Errors <- err

		// don't let the exponential backoff go to over 64 seconds...
		if backoff < (time.Millisecond * 64000) {
			backoff *= 2
		}
	}
}
