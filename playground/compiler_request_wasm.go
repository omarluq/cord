//go:build js && wasm

package playground

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"syscall/js"

	"github.com/omarluq/cord/playground/internal/protocol"
)

func performCompilationRequest(
	ctx context.Context,
	endpoint string,
	body []byte,
) (*http.Response, error) {
	fetchContext, cancel := context.WithTimeout(ctx, compilationRequestTimeout)
	defer cancel()

	options := js.Global().Get("Object").New()
	options.Set("method", http.MethodPost)
	headers := js.Global().Get("Headers").New()
	headers.Call("set", "Content-Type", protocol.JSONMediaType)
	options.Set("headers", headers)

	requestBody := js.Global().Get("Uint8Array").New(len(body))
	if js.CopyBytesToJS(requestBody, body) != len(body) {
		return nil, errors.New("copy compilation request to JavaScript")
	}
	options.Set("body", requestBody)

	abortController := js.Global().Get("AbortController").New()
	options.Set("signal", abortController.Get("signal"))

	result, err := awaitPromise(
		fetchContext,
		js.Global().Call("fetch", endpoint, options),
		abortController,
	)
	if err != nil {
		abortController.Call("abort")

		return nil, fmt.Errorf("fetch compilation response: %w", err)
	}

	arrayBuffer, err := awaitPromise(
		fetchContext,
		result.Call("arrayBuffer"),
		abortController,
	)
	if err != nil {
		abortController.Call("abort")

		return nil, fmt.Errorf("read compilation response: %w", err)
	}

	responseBody := js.Global().Get("Uint8Array").New(arrayBuffer)
	content := make([]byte, responseBody.Get("byteLength").Int())
	if js.CopyBytesToGo(content, responseBody) != len(content) {
		return nil, errors.New("copy compilation response from JavaScript")
	}

	response := &http.Response{
		StatusCode: result.Get("status").Int(),
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(content)),
	}
	response.Status = fmt.Sprintf("%d %s", response.StatusCode, http.StatusText(response.StatusCode))

	headerIterator := result.Get("headers").Call("entries")
	for {
		next := headerIterator.Call("next")
		if next.Get("done").Bool() {
			break
		}

		pair := next.Get("value")
		response.Header.Add(pair.Index(0).String(), pair.Index(1).String())
	}

	response.ContentLength = int64(len(content))

	return response, nil
}

func awaitPromise(
	ctx context.Context,
	promise js.Value,
	abortController js.Value,
) (js.Value, error) {
	result := make(chan js.Value, 1)
	failure := make(chan error, 1)

	var resolve, reject js.Func
	resolve = js.FuncOf(func(_ js.Value, args []js.Value) any {
		result <- args[0]

		return nil
	})
	reject = js.FuncOf(func(_ js.Value, args []js.Value) any {
		failure <- errors.New(args[0].Call("toString").String())

		return nil
	})
	defer resolve.Release()
	defer reject.Release()

	promise.Call("then", resolve, reject)

	select {
	case value := <-result:
		return value, nil
	case err := <-failure:
		return js.Undefined(), err
	case <-ctx.Done():
		abortController.Call("abort")
		// Keep the callbacks alive until abort settles the promise. Releasing a
		// js.Func while JavaScript may still invoke it causes a runtime panic.
		select {
		case <-result:
		case <-failure:
		}

		return js.Undefined(), ctx.Err()
	}
}
