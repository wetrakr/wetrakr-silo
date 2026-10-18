package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	defaultBaseURL        = "https://api.wetrakr.com"
	defaultRequestTimeout = 20 * time.Second
	apiVersion            = "1"

	// A compact page of 5,000 rows is the largest response the plugin reads,
	// about 1.5 MB.
	maxResponseBytes = 32 << 20
	// maxErrorBody bounds how much of a failed response is read for its error
	// code.
	maxErrorBody = 4 << 10

	// statusPlanLimit is WeTrakr's 420, sent when a free account is over a
	// plan quota (lists, items, notes). net/http has no constant.
	statusPlanLimit = 420

	// inPlaceRetryReserve is the time a retried request needs after its wait;
	// a wait that would not leave it before the deadline defers instead.
	inPlaceRetryReserve = 5 * time.Second
)

// apiClient sends the requests of one RPC: the app key and, after sign-in,
// the connection's access token.
type apiClient struct {
	server  *Server
	app     appConfig
	token   string
	baseURL string
}

// request is one WeTrakr call.
type request struct {
	method string
	path   string
	query  url.Values
	body   any
	// idempotencyKey makes a write safe to retry: WeTrakr replays the first
	// answer for 24 hours instead of running the write again.
	idempotencyKey string
}

// response is what a successful call returned besides its decoded body.
type response struct {
	status int
	header http.Header
}

// do sends one WeTrakr request and decodes a successful JSON response into
// out. Requests are paced per access token, and a 429 with a short
// Retry-After is retried in place with the same body.
func (c *apiClient) do(ctx context.Context, req request, out any) (response, *pluginv1.WatchSyncFault) {
	var body []byte
	if req.body != nil {
		encoded, err := json.Marshal(req.body)
		if err != nil {
			return response{}, permanentFault("WeTrakr request could not be encoded")
		}
		body = encoded
	}
	limiter := c.server.reads
	if req.method != http.MethodGet {
		limiter = c.server.writes
	}
	for attempt := 0; ; attempt++ {
		if c.token != "" {
			if err := limiter.Wait(ctx, c.token); err != nil {
				return response{}, limiterWaitFault(ctx)
			}
		}
		result, fault := c.roundTrip(ctx, req, body, out)
		if fault != nil {
			return response{}, fault
		}
		if result.status != http.StatusTooManyRequests {
			if result.failed() {
				return response{status: result.status, header: result.header}, responseFault(req.method, req.path, c.token != "", result)
			}
			return response{status: result.status, header: result.header}, nil
		}
		wait := result.retryAfter()
		if attempt < maxRetryAttempts && wait <= maxInPlaceRetryWait && fitsBeforeDeadline(ctx, wait) {
			if err := c.server.sleep(ctx, wait); err != nil {
				return response{}, deadlineFault()
			}
			continue
		}
		if attempt >= maxRetryAttempts && wait < defaultRetryAfter {
			wait = defaultRetryAfter
		}
		return response{status: result.status, header: result.header}, rateLimitedFault(wait, result.errorCode())
	}
}

type httpResult struct {
	status int
	header http.Header
	// errorBody is the start of a failed response's body. It is parsed for an
	// error code and never shown.
	errorBody []byte
	now       time.Time
}

func (r httpResult) failed() bool {
	return r.status < http.StatusOK || r.status >= http.StatusMultipleChoices
}

// retryAfter is a 429's wait: Retry-After, or for a daily quota the seconds
// until it resets (X-Quota-Reset), or defaultRetryAfter.
func (r httpResult) retryAfter() time.Duration {
	if wait, ok := parseRetryAfter(r.header.Get("Retry-After"), r.now); ok {
		return wait
	}
	switch r.errorCode() {
	case "QUOTA_EXCEEDED", "APP_QUOTA_EXCEEDED", "APP_USER_QUOTA_EXCEEDED":
		if wait, ok := parseRetryAfter(r.header.Get("X-Quota-Reset"), r.now); ok && wait > 0 {
			return wait
		}
		return untilMidnightUTC(r.now)
	}
	return defaultRetryAfter
}

func untilMidnightUTC(now time.Time) time.Duration {
	now = now.UTC()
	midnight := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC)
	return midnight.Sub(now)
}

// roundTrip performs a single HTTP attempt. A transport failure is a fault;
// any HTTP status is a result for the caller to map.
func (c *apiClient) roundTrip(ctx context.Context, req request, body []byte, out any) (httpResult, *pluginv1.WatchSyncFault) {
	base := c.baseURL
	if base == "" {
		base = c.server.baseURL
	}
	endpoint := strings.TrimRight(base, "/") + req.path
	if len(req.query) > 0 {
		endpoint += "?" + req.query.Encode()
	}
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	httpReq, err := http.NewRequestWithContext(ctx, req.method, endpoint, reader)
	if err != nil {
		return httpResult{}, permanentFault("WeTrakr request could not be created")
	}
	if body != nil {
		httpReq.Header.Set("Content-Type", "application/json")
	}
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("wetrakr-api-version", apiVersion)
	httpReq.Header.Set("wetrakr-api-key", c.app.clientID)
	httpReq.Header.Set("User-Agent", c.server.userAgent)
	if c.token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.token)
	}
	if req.idempotencyKey != "" {
		httpReq.Header.Set("Idempotency-Key", req.idempotencyKey)
	}
	resp, err := c.server.http.Do(httpReq)
	if err != nil {
		if ctx.Err() != nil {
			return httpResult{}, deadlineFault()
		}
		return httpResult{}, temporaryFault("WeTrakr is temporarily unreachable")
	}
	defer func() { _ = resp.Body.Close() }()
	result := httpResult{status: resp.StatusCode, header: resp.Header, now: c.server.clock()}
	if result.failed() {
		result.errorBody, _ = io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		return result, nil
	}
	if out != nil && resp.StatusCode != http.StatusNoContent {
		if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(out); err != nil {
			if ctx.Err() != nil {
				return httpResult{}, deadlineFault()
			}
			return httpResult{}, temporaryFault("WeTrakr returned an unreadable response")
		}
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorBody))
	return result, nil
}

var errorCodePattern = regexp.MustCompile(`^[A-Za-z_]{1,64}$`)

// errorCode returns the machine-readable code of a WeTrakr error body. Errors
// carry it as a string, { "error": "QUOTA_EXCEEDED" }, or in newer endpoints
// as an object, { "error": { "code": "INVALID_MEDIA_ID" } }. Only a
// well-formed code is returned; the free-text message never is.
func (r httpResult) errorCode() string {
	return errorCodeFrom(r.errorBody)
}

func errorCodeFrom(body []byte) string {
	var envelope struct {
		Error json.RawMessage `json:"error"`
	}
	if json.Unmarshal(body, &envelope) != nil || len(envelope.Error) == 0 {
		return ""
	}
	var code string
	if json.Unmarshal(envelope.Error, &code) != nil {
		var nested struct {
			Code string `json:"code"`
		}
		if json.Unmarshal(envelope.Error, &nested) != nil {
			return ""
		}
		code = nested.Code
	}
	if !errorCodePattern.MatchString(code) {
		return ""
	}
	return code
}

// responseFault describes a failed WeTrakr response. A rejected access or
// refresh token is an invalid credential, so the profile owner is told to
// reconnect. App and account refusals say who can fix them.
func responseFault(method, path string, sentToken bool, result httpResult) *pluginv1.WatchSyncFault {
	failed := fmt.Sprintf("WeTrakr request %s %s failed (HTTP %d)", method, path, result.status)
	code := result.errorCode()
	if code != "" {
		failed += " (" + code + ")"
	}
	switch {
	case code == "INVALID_REFRESH_TOKEN", result.status == http.StatusUnauthorized && sentToken:
		return newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL,
			failed+": WeTrakr no longer accepts this connection's sign-in; reconnect WeTrakr")
	case result.status == http.StatusUnauthorized:
		return newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED,
			failed+": WeTrakr rejected the app key; an administrator should check the client ID in the plugin settings")
	case result.status == http.StatusForbidden:
		return newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED,
			failed+": WeTrakr refused this request for the app or the account")
	case result.status == statusPlanLimit:
		return newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED,
			failed+": the WeTrakr account has reached a free plan limit; WeTrakr VIP raises it")
	case result.status == http.StatusLocked:
		return newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED,
			failed+": the WeTrakr app key is locked; contact support@wetrakr.com")
	case result.status == http.StatusUpgradeRequired:
		return newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED,
			failed+": this WeTrakr feature needs WeTrakr VIP")
	case result.status == http.StatusNotFound:
		return newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST,
			failed+": WeTrakr could not find the item")
	case result.status == http.StatusRequestEntityTooLarge, result.status == http.StatusUnprocessableEntity:
		return newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST, failed)
	case result.status == http.StatusBadRequest:
		// A 400 names a bug in the request, not a passing failure: retrying the
		// same body cannot succeed.
		return newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST, failed)
	default:
		// 409 (idempotent write still in progress), 5xx and Cloudflare errors
		// stay retryable.
		return temporaryFault(failed)
	}
}

// fitsBeforeDeadline reports whether a wait still leaves a request time to
// finish before ctx's deadline.
func fitsBeforeDeadline(ctx context.Context, wait time.Duration) bool {
	deadline, ok := ctx.Deadline()
	return !ok || time.Until(deadline) >= wait+inPlaceRetryReserve
}

// limiterWaitFault classifies a failed limiter wait. The limiter refuses when
// the next slot lies past ctx's deadline: the request was never sent, so the
// connection is deferred like a 429 instead of failing.
func limiterWaitFault(ctx context.Context) *pluginv1.WatchSyncFault {
	if ctx.Err() != nil {
		return deadlineFault()
	}
	return rateLimitedFault(writeInterval, "")
}

func newFault(code pluginv1.WatchSyncFaultCode, message string) *pluginv1.WatchSyncFault {
	return &pluginv1.WatchSyncFault{Code: code, SafeMessage: message}
}

func temporaryFault(message string) *pluginv1.WatchSyncFault {
	return newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY, message)
}

func permanentFault(message string) *pluginv1.WatchSyncFault {
	return newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMANENT, message)
}

func invalidRequestFault(message string) *pluginv1.WatchSyncFault {
	return newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST, message)
}

func deadlineFault() *pluginv1.WatchSyncFault {
	return temporaryFault("WeTrakr did not answer before the sync deadline")
}

func rateLimitedFault(retryAfter time.Duration, code string) *pluginv1.WatchSyncFault {
	message := "WeTrakr rate limit reached"
	switch code {
	case "QUOTA_EXCEEDED":
		message = "The WeTrakr account used up its daily API quota; it resets at midnight UTC"
	case "APP_QUOTA_EXCEEDED":
		message = "The WeTrakr app used up its daily request limit; it resets at midnight UTC"
	case "APP_USER_QUOTA_EXCEEDED":
		message = "This WeTrakr account reached the daily request limit for Silo; it resets at midnight UTC"
	}
	return &pluginv1.WatchSyncFault{
		Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED,
		SafeMessage: message,
		RetryAfter:  durationpb.New(retryAfter),
	}
}

// connectionWide reports whether a fault concerns the whole connection rather
// than one event: it belongs on the ApplyEvents response.
func connectionWide(f *pluginv1.WatchSyncFault) bool {
	switch f.GetCode() {
	case pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL,
		pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED,
		pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED:
		return true
	default:
		return false
	}
}

// nextCursor is the compact-page cursor WeTrakr sends while more rows remain.
func nextCursor(header http.Header) string {
	return strings.TrimSpace(header.Get("X-Pagination-Next"))
}

func intString(value int) string {
	if value == 0 {
		return ""
	}
	return strconv.Itoa(value)
}

func parseInt(value string) int {
	parsed, _ := strconv.Atoi(strings.TrimSpace(value))
	return parsed
}
