/*
	Metrics to be saved in Redis

	CREATE TABLE "metrics" (
		"timestamp" TIMESTAMP,
		"labels_hash" STRING,
		"labels" OBJECT(DYNAMIC),
		"value" DOUBLE,
		"valueRaw" LONG,
		PRIMARY KEY ("timestamp", "labels_hash")
	);

	RedisPrometheusAdapter_1  | Request shown below
RedisPrometheusAdapter_1  | {Queries:[start_timestamp_ms:1529355148601 end_timestamp_ms:1529355448601 matchers:<name:"__name__" value:"prometheus_tsdb_compaction_chunk_samples_bucket" > ]}
RedisPrometheusAdapter_1  | Request shown below
RedisPrometheusAdapter_1  |
{Results:[
	timeseries:<
	labels:<name:"__name__" value:"prometheus_tsdb_compaction_chunk_samples_bucket" >
	labels:<name:"instance" value:"localhost:9090" >
	labels:<name:"job" value:"prometheus" >
	labels:<name:"le" value:"+Inf" >
	samples:<timestamp_ms:1529355153029 >
	samples:<timestamp_ms:1529355158029 >
	samples:<timestamp_ms:1529355163029 >
	samples:<timestamp_ms:1529355168029 >
	samples:<timestamp_ms:1529355173029 >
	samples:<timestamp_ms:1529355178029 >
	samples:<timestamp_ms:1529355183029 >
	samples:<timestamp_ms:1529355188029 >
	samples:<timestamp_ms:1529355193029 >
	samples:<timestamp_ms:1529355198029 >
	samples:<timestamp_ms:1529355203029 >
	samples:<timestamp_ms:1529355208029 >
	samples:<timestamp_ms:1529355213029 >
	samples:<timestamp_ms:1529355218029 >
	samples:<timestamp_ms:1529355223029 >
	samples:<timestamp_ms:1529355228029 >
	samples:<timestamp_ms:1529355233029 >
	samples:<timestamp_ms:1529355238029 >
	samples:<timestamp_ms:1529355243029 >
	samples:<timestamp_ms:1529355248029 >
	samples:<timestamp_ms:1529355253029 >
	samples:<timestamp_ms:1529355258029 >
	samples:<timestamp_ms:1529355263029 >
	samples:<timestamp_ms:1529355268029 >
	samples:<timestamp_ms:1529355273029 >
	samples:<timestamp_ms:1529355278029 >
	samples:<timestamp_ms:1529355283029 >
	samples:<timestamp_ms:1529355288029 >
	samples:<timestamp_ms:1529355293029 >
	samples:<timestamp_ms:1529355298029 >
	samples:<timestamp_ms:1529355303029 >
	samples:<timestamp_ms:1529355308029 >
	samples:<timestamp_ms:1529355313029 >
	samples:<timestamp_ms:1529355318029 >
	samples:<timestamp_ms:1529355323029 >
	samples:<timestamp_ms:1529355328029 >
	samples:<timestamp_ms:1529355369098 >
	samples:<timestamp_ms:1529355374098 >
	samples:<timestamp_ms:1529355379098 >
	samples:<timestamp_ms:1529355384098 >
	samples:<timestamp_ms:1529355389098 >
	samples:<timestamp_ms:1529355394098 >
	samples:<timestamp_ms:1529355399098 >
	samples:<timestamp_ms:1529355404098 >
	samples:<timestamp_ms:1529355409098 >
	samples:<timestamp_ms:1529355414098 >
	samples:<timestamp_ms:1529355419098 >
	samples:<timestamp_ms:1529355424098 >
	samples:<timestamp_ms:1529355429098 >
	samples:<timestamp_ms:1529355434098 >
	samples:<timestamp_ms:1529355439098 > >

	timeseries:<
	labels:<name:"__name__" value:"http_requests_total" >
	labels:<name:"cpu" value:"real-time" >
	labels:<name:"instance" value:"di-9990.net.com">
	labels:<name:"job" value:"prometheus" >
	labels:<name:"region" value:"eu-west-1" >
	samples:<value:1.2e-322 timestamp_ms:1529302504 >
	samples:<value:1.33e-322 timestamp_ms:1529302507 >
	samples:<value:1.04e-322 timestamp_ms:1529302510 >
	samples:<value:1.04e-322 timestamp_ms:1529302513 > >

*/

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/kit/endpoint"

	/*
		Package sd provides utilities related to service discovery.
		That includes the client-side loadbalancer pattern, where a
		microservice subscribes to a service discovery system in
		order to reach remote instances; as well as the registrator
		pattern, where a microservice registers itself in a service discovery
		system. Implementations are provided for most common systems.
	*/
	"github.com/go-kit/kit/sd"
	/* Package lb implements the client-side load balancer pattern. */
	"github.com/go-kit/kit/sd/lb"
	/*
		Package http provides a general purpose HTTP binding for endpoints.
	*/
	httptransport "github.com/go-kit/kit/transport/http"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/remote"
)

var (
	// Flags for running the binary
	listenAddress = flag.String("web.listen-address", ":9783", "Address to listen on for Prometheus requests.")
	reddisURL     = flag.String("reddis.url", "http://redis:6379", "URL to send Crate SQL to. Can list multiple URLs comma seperated.")

	writeDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "crate_adapter_write_latency_seconds",
		Help: "How long it took us to respond to write requests.",
	})
	writeErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "crate_adapter_write_failed_total",
		Help: "How many write request we returned errors for.",
	})
	writeSamples = prometheus.NewSummary(prometheus.SummaryOpts{
		Name: "crate_adapter_write_timeseries_samples",
		Help: "How many samples each written timeseries has.",
	})
	writeCrateDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "crate_adapter_write_crate_latency_seconds",
		Help: "Latency for inserts to Crate.",
	})
	writeCrateErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "crate_adapter_write_crate_failed_total",
		Help: "How many inserts to Crate failed.",
	})
	readDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "crate_adapter_read_latency_seconds",
		Help: "How long it took us to respond to read requests.",
	})
	readErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "crate_adapter_read_failed_total",
		Help: "How many read requests we returned errors for.",
	})
	readCrateDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "crate_adapter_read_crate_latency_seconds",
		Help: "Latency for selects from Crate.",
	})
	readCrateErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "crate_adapter_read_crate_failed_total",
		Help: "How many selects from Crate failed.",
	})
	readSamples = prometheus.NewSummary(prometheus.SummaryOpts{
		Name: "crate_adapter_read_timeseries_samples",
		Help: "How many samples each returned timeseries has.",
	})
)

func init() {
	prometheus.MustRegister(writeDuration)
	prometheus.MustRegister(writeErrors)
	prometheus.MustRegister(writeSamples)
	prometheus.MustRegister(writeCrateDuration)
	prometheus.MustRegister(writeCrateErrors)
	prometheus.MustRegister(readDuration)
	prometheus.MustRegister(readErrors)
	prometheus.MustRegister(readSamples)
	prometheus.MustRegister(readCrateDuration)
	prometheus.MustRegister(readCrateErrors)
}

// Escaping for strings for Crate.io SQL.
var escaper = strings.NewReplacer("\\", "\\\\", "\"", "\\\"", "'", "\\'")

// Escape a labelname for use in SQL as a column name.
func escapeLabelName(s string) string {
	return "labels['" + escaper.Replace(s) + "']"
}

// Escape a labelvalue for use in SQL as a string value.
func escapeLabelValue(s string) string {
	return "'" + escaper.Replace(s) + "'"
}

type crateRequest struct {
	Stmt     string          `json:"stmt"`
	BulkArgs [][]interface{} `json:"bulk_args,omitempty"`
}

type crateResponse struct {
	Cols []string        `json:"cols,omitempty"`
	Rows [][]interface{} `json:"rows,omitempty"`
}

// Convert a read query into a Crate SQL query.
func queryToSQL(q *remote.Query) (string, error) {
	selectors := make([]string, 0, len(q.Matchers)+2)
	for _, m := range q.Matchers {
		switch m.Type {
		case remote.MatchType_EQUAL:
			if m.Value == "" {
				// Empty labels are recorded as NULL.
				// In PromQL, empty labels and missing labels are the same thing.
				selectors = append(selectors, fmt.Sprintf("(%s IS NULL)", escapeLabelName(m.Name)))
			} else {
				selectors = append(selectors, fmt.Sprintf("(%s = %s)", escapeLabelName(m.Name), escapeLabelValue(m.Value)))
			}
		case remote.MatchType_NOT_EQUAL:
			if m.Value == "" {
				selectors = append(selectors, fmt.Sprintf("(%s IS NOT NULL)", escapeLabelName(m.Name)))
			} else {
				selectors = append(selectors, fmt.Sprintf("(%s != %s)", escapeLabelName(m.Name), escapeLabelValue(m.Value)))
			}
		case remote.MatchType_REGEX_MATCH:
			re := "^(?:" + m.Value + ")$"
			matchesEmpty, err := regexp.MatchString(re, "")
			if err != nil {
				return "", err
			}
			// Crate regexes are not RE2, so there may be small semantic differences here.
			if matchesEmpty {
				selectors = append(selectors, fmt.Sprintf("(%s ~ %s OR %s IS NULL)", escapeLabelName(m.Name), escapeLabelValue(re), escapeLabelName(m.Name)))
			} else {
				selectors = append(selectors, fmt.Sprintf("(%s ~ %s)", escapeLabelName(m.Name), escapeLabelValue(re)))
			}
		case remote.MatchType_REGEX_NO_MATCH:
			re := "^(?:" + m.Value + ")$"
			matchesEmpty, err := regexp.MatchString(re, "")
			if err != nil {
				return "", err
			}
			if matchesEmpty {
				selectors = append(selectors, fmt.Sprintf("(%s !~ %s)", escapeLabelName(m.Name), escapeLabelValue(re)))
			} else {
				selectors = append(selectors, fmt.Sprintf("(%s !~ %s OR %s IS NULL)", escapeLabelName(m.Name), escapeLabelValue(re), escapeLabelName(m.Name)))
			}
		}
	}
	selectors = append(selectors, fmt.Sprintf("(timestamp <= %d)", q.EndTimestampMs))
	selectors = append(selectors, fmt.Sprintf("(timestamp >= %d)", q.StartTimestampMs))

	return fmt.Sprintf("SELECT * from metrics WHERE %s ORDER BY timestamp", strings.Join(selectors, " AND ")), nil
}

/*
	type TimeSeries struct {
		Labels  []*Label  `protobuf:"bytes,1,rep,name=labels" json:"labels,omitempty"`
		Samples []*Sample `protobuf:"bytes,2,rep,name=samples" json:"samples,omitempty"`
	}
*/
func responseToTimeseries(data *crateResponse) []*remote.TimeSeries {
	timeseries := map[string]*remote.TimeSeries{}
	for _, row := range data.Rows {
		//type LabelSet map[LabelName]LabelValue
		metric := model.Metric{}
		var v float64
		var t int64
		for i, value := range row {
			// time.Sleep(5 * time.Second)
			switch data.Cols[i] {
			case "labels":
				labels := value.(map[string]interface{})
				for k, v := range labels {
					// lfoo -> foo.
					metric[model.LabelName(k)] = model.LabelValue(v.(string))
				}
			case "timestamp":
				t, _ = value.(json.Number).Int64()
			case "valueRaw":
				val, _ := value.(json.Number).Int64()
				v = math.Float64frombits(uint64(val))
			}
		}
		ts, ok := timeseries[metric.String()]
		if !ok {
			ts = &remote.TimeSeries{}
			labelnames := make([]string, 0, len(metric))
			for k := range metric {
				labelnames = append(labelnames, string(k))
			}
			sort.Strings(labelnames) // Sort for unittests.
			for _, k := range labelnames {
				ts.Labels = append(ts.Labels, &remote.LabelPair{Name: string(k), Value: string(metric[model.LabelName(k)])})
			}
			timeseries[metric.String()] = ts
		}
		ts.Samples = append(ts.Samples, &remote.Sample{Value: v, TimestampMs: t})
	}

	names := make([]string, 0, len(timeseries))
	for k := range timeseries {
		names = append(names, k)
	}
	sort.Strings(names)
	resp := make([]*remote.TimeSeries, 0, len(timeseries))
	for _, name := range names {
		writeSamples.Observe(float64(len(timeseries[name].Samples)))
		resp = append(resp, timeseries[name])
	}
	return resp
}

type redisAdapter struct {
	// Endpoint is the fundamental building block of servers and clients. It represents a single RPC method.
	ep endpoint.Endpoint
}

func (ca *redisAdapter) runQuery(q *remote.Query) ([]*remote.TimeSeries, error) {
	// query, err := queryToSQL(q)
	// if err != nil {
	// 	return nil, err
	// }

	// request := crateRequest{Stmt: query}

	// timer := prometheus.NewTimer(readCrateDuration)
	// result, err := ca.ep(context.Background(), request)
	// timer.ObserveDuration()
	// if err != nil {
	// 	readCrateErrors.Inc()
	// 	return nil, err
	// }
	// timeseries := responseToTimeseries(result.(*crateResponse))
	var response crateResponse
	response.Cols = []string{
		"labels",
		"timestamp",
		"valueRaw",
	}

	/*Results:[timeseries:<
	labels:<name:"__name__" value:"prometheus_tsdb_compaction_chunk_samples_bucket" >
	labels:<name:"instance" value:"localhost:9090" >
	labels:<name:"job" value:"prometheus" >
	labels:<name:"le" value:"+Inf" >
	labels:<name:"region" value:"eu-west-1" >
	samples:<timestamp_ms:1529359077 >
	samples:<timestamp_ms:1529359078 >
	samples:<timestamp_ms:1529359078 >
	samples:<timestamp_ms:1529359078 >
	samples:<timestamp_ms:1529359079 >
	samples:<timestamp_ms:1529359079 >
	samples:<timestamp_ms:1529359079 >
	samples:<timestamp_ms:1529359080 >
	samples:<timestamp_ms:1529359080 >
	samples:<timestamp_ms:1529359080 >
	samples:<timestamp_ms:1529359080 >
	samples:<timestamp_ms:1529359081 >
	samples:<timestamp_ms:1529359081 >
	samples:<timestamp_ms:1529359081 >
	samples:<timestamp_ms:1529359082 >
	samples:<timestamp_ms:1529359082 >
	samples:<timestamp_ms:1529359082 > > ]*/

	/*
		timeseries:<
			labels:<name:"__name__" value:"prometheus_tsdb_compaction_chunk_samples_bucket" >
			labels:<name:"instance" value:"localhost:9090" >
			labels:<name:"job" value:"prometheus" >
			labels:<name:"le" value:"+Inf" >
			samples:<timestamp_ms:1529355153029 >
			samples:<timestamp_ms:1529355158029 >
			samples:<timestamp_ms:1529355163029 >
			samples:<timestamp_ms:1529355168029 >
			samples:<timestamp_ms:1529355173029 >
			samples:<timestamp_ms:1529355178029 >
			samples:<timestamp_ms:1529355183029 >
			samples:<timestamp_ms:1529355188029 >
			samples:<timestamp_ms:1529355193029 >
			samples:<timestamp_ms:1529355198029 >
			samples:<timestamp_ms:1529355203029 >
			samples:<timestamp_ms:1529355208029 >
			samples:<timestamp_ms:1529355213029 >
			samples:<timestamp_ms:1529355218029 >
			samples:<timestamp_ms:1529355223029 >
			samples:<timestamp_ms:1529355228029 >
			samples:<timestamp_ms:1529355233029 >
			samples:<timestamp_ms:1529355238029 >
			samples:<timestamp_ms:1529355243029 >
			samples:<timestamp_ms:1529355248029 >
			samples:<timestamp_ms:1529355253029 >
			samples:<timestamp_ms:1529355258029 >
			samples:<timestamp_ms:1529355263029 >
			samples:<timestamp_ms:1529355268029 >
			samples:<timestamp_ms:1529355273029 >
			samples:<timestamp_ms:1529355278029 >
			samples:<timestamp_ms:1529355283029 >
			samples:<timestamp_ms:1529355288029 >
			samples:<timestamp_ms:1529355293029 >
			samples:<timestamp_ms:1529355298029 >
			samples:<timestamp_ms:1529355303029 >
			samples:<timestamp_ms:1529355308029 >
			samples:<timestamp_ms:1529355313029 >
			samples:<timestamp_ms:1529355318029 >
			samples:<timestamp_ms:1529355323029 >
			samples:<timestamp_ms:1529355328029 >
			samples:<timestamp_ms:1529355369098 >
			samples:<timestamp_ms:1529355374098 >
			samples:<timestamp_ms:1529355379098 >
			samples:<timestamp_ms:1529355384098 >
			samples:<timestamp_ms:1529355389098 >
			samples:<timestamp_ms:1529355394098 >
			samples:<timestamp_ms:1529355399098 >
			samples:<timestamp_ms:1529355404098 >
			samples:<timestamp_ms:1529355409098 >
			samples:<timestamp_ms:1529355414098 >
			samples:<timestamp_ms:1529355419098 >
			samples:<timestamp_ms:1529355424098 >
			samples:<timestamp_ms:1529355429098 >
			samples:<timestamp_ms:1529355434098 >
			samples:<timestamp_ms:1529355439098 > >
	*/
	response.Rows = [][]interface{}{
		[]interface{}{
			map[string]interface{}{
				"le":       "+Inf",
				"region":   "eu-west-1",
				"instance": "localhost:9090",
				"job":      "prometheus",
				"__name__": "prometheus_tsdb_compaction_chunk_samples_bucket",
			},
			json.Number("1529355153029"),
			json.Number("0"),
		},
		[]interface{}{
			map[string]interface{}{
				"le":       "+Inf",
				"region":   "eu-west-1",
				"instance": "localhost:9090",
				"job":      "prometheus",
				"__name__": "prometheus_tsdb_compaction_chunk_samples_bucket",
			},
			// json.Number("1529355159029"),
			json.Number("1529355153029"),
			json.Number("0"),
		},
	}

	timeseries := responseToTimeseries(&response)
	return timeseries, nil
}

func (ca *redisAdapter) handleRead(w http.ResponseWriter, r *http.Request) {
	// Measure time taken to read a resquest. Exposabe on /metrics
	timer := prometheus.NewTimer(readDuration)
	defer timer.ObserveDuration()

	// Ready the compressed buffer
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.With("err", err).Error("Failed to read body.")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Decode buffer with snappy
	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		log.With("err", err).Error("Failed to decompress body.")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Unmarshal the protobuf request into a ReadRequest object
	/*
		type LabelMatcher struct {
			Type  LabelMatcher_Type `protobuf:"varint,1,opt,name=type,proto3,enum=prometheus.LabelMatcher_Type" json:"type,omitempty"`
			Name  string            `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
			Value string            `protobuf:"bytes,3,opt,name=value,proto3" json:"value,omitempty"`
		}
		type Query struct {
			StartTimestampMs int64           `protobuf:"varint,1,opt,name=start_timestamp_ms,json=startTimestampMs,proto3" json:"start_timestamp_ms,omitempty"`
			EndTimestampMs   int64           `protobuf:"varint,2,opt,name=end_timestamp_ms,json=endTimestampMs,proto3" json:"end_timestamp_ms,omitempty"`
			Matchers         []*LabelMatcher `protobuf:"bytes,3,rep,name=matchers" json:"matchers,omitempty"`
			Hints            *ReadHints      `protobuf:"bytes,4,opt,name=hints" json:"hints,omitempty"`
		}
		type ReadRequest struct {
			Queries []*Query `protobuf:"bytes,1,rep,name=queries" json:"queries,omitempty"`
		}
	*/
	var req remote.ReadRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		log.With("err", err).Error("Failed to unmarshal body.")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	/*
		req looks like
		{
			[
				start_timestamp_ms:1529293152950
				end_timestamp_ms:1529293452950
				matchers:<name:"job" value:"prometheus" >
				matchers:<name:"group" value:"canary" >
				matchers:<name:"__name__" value:"http_requests_total" >
				matchers:<name:"region" value:"eu-west-1" >
				matchers:<name:"env" value:"prod" >
			]
		}
	*/

	fmt.Println(req)

	// May only handle a single query. Early return otherwise.
	if len(req.Queries) != 1 {
		log.Error("More than one query sent.")
		http.Error(w, "Can only handle one query.", http.StatusBadRequest)
		return
	}

	result, err := ca.runQuery(req.Queries[0])
	if err != nil {
		log.With("err", err).Error("Failed to run select against Crate.")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := remote.ReadResponse{
		Results: []*remote.QueryResult{
			{Timeseries: result},
		},
	}
	fmt.Printf("%+v\n", resp)
	data, err := proto.Marshal(&resp)
	if err != nil {
		log.With("err", err).Error("Failed to marshal response.")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	if _, err := w.Write(snappy.Encode(nil, data)); err != nil {
		log.With("err", err).Error("Failed to compress response.")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func writesToCrateRequest(req *remote.WriteRequest) *crateRequest {
	request := &crateRequest{
		BulkArgs: make([][]interface{}, 0, len(req.Timeseries)),
	}
	request.Stmt = fmt.Sprintf(`INSERT INTO metrics ("labels", "labels_hash", "value", "valueRaw", "timestamp") VALUES (?, ?, ?, ?, ?)`)

	for _, ts := range req.Timeseries {
		metric := make(model.Metric, len(ts.Labels))
		for _, l := range ts.Labels {
			metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		}

		for _, s := range ts.Samples {
			args := make([]interface{}, 0, 5)
			args = append(args, metric)
			args = append(args, metric.Fingerprint().String())
			// Convert to string to handle NaN/Inf/-Inf.
			switch {
			case math.IsInf(s.Value, 1):
				args = append(args, "Infinity")
			case math.IsInf(s.Value, -1):
				args = append(args, "-Infinity")
			default:
				args = append(args, fmt.Sprintf("%f", s.Value))
			}
			// Crate.io can't handle full NaN values as required by Prometheus 2.0,
			// so store the raw bits as an int64.
			args = append(args, int64(math.Float64bits(s.Value)))
			args = append(args, s.TimestampMs)

			request.BulkArgs = append(request.BulkArgs, args)
		}
		writeSamples.Observe(float64(len(ts.Samples)))
	}
	return request
}

func (ca *redisAdapter) handleWrite(w http.ResponseWriter, r *http.Request) {
	timer := prometheus.NewTimer(writeDuration)
	defer timer.ObserveDuration()

	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.With("err", err).Error("Failed to read body")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		log.With("err", err).Error("Failed to decompress body")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req remote.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		log.With("err", err).Error("Failed to unmarshal body")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	request := writesToCrateRequest(&req)

	writeTimer := prometheus.NewTimer(writeCrateDuration)
	_, err = ca.ep(context.Background(), request)
	writeTimer.ObserveDuration()
	if err != nil {
		writeCrateErrors.Inc()
		log.With("err", err).Error("Failed to POST inserts to Crate.")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// EncodeRequestFunc encodes the passed request object into the HTTP request object.
// It's designed to be used in HTTP clients, for client-side endpoints.
// One straightforward EncodeRequestFunc could be something that JSON encodes the object directly to the request body.
func encodeRedisRequest(_ context.Context, r *http.Request, request interface{}) error {
	jsonRequest, err := json.Marshal(request)
	if err != nil {
		return err
	}
	log.With("json", string(jsonRequest)).Debug("Request to Crate")
	r.Body = ioutil.NopCloser(bytes.NewBuffer(jsonRequest))
	r.Header.Set("Content-Type", "application/json; charset=utf-8")
	return nil
}

// EncodeResponseFunc encodes the passed response object to the HTTP response writer.
// It's designed to be used in HTTP servers, for server-side endpoints.
// One straightforward EncodeResponseFunc could be something that JSON encodes the object directly to the response body.
func decodeRedisResponse(_ context.Context, r *http.Response) (interface{}, error) {
	var response crateResponse
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
	if err := decoder.Decode(&response); err != nil {
		return nil, err
	}
	return &response, nil
}

func main() {
	flag.Parse()

	urls := strings.Split(*reddisURL, ",")
	if len(urls) == 0 {
		log.Fatal("No URLs provided in -redis.url.")
	}

	// FixedEndpointer yields a fixed set of endpoints.
	// adding all endpoints to the list of subscribers
	subscriber := sd.FixedEndpointer{}
	for _, u := range urls {
		url, err := url.Parse(u)
		if err != nil {
			log.Fatal("Invalid URL %q: %s", url, err)
		}
		// create endpoint with the redis encoders and decoders
		// NewClient constructs a usable Client for a single remote method.
		ep := httptransport.NewClient(
			"POST",
			url,
			encodeRedisRequest,
			decodeRedisResponse).Endpoint()
		subscriber = append(subscriber, ep)
	}

	balancer := lb.NewRoundRobin(subscriber)
	// Turn into a service oriented load balancer with retries.
	retry := lb.Retry(len(urls), 1*time.Minute, balancer)

	ca := redisAdapter{
		ep: retry,
	}
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(
			`<html>
			<head><title>Redis Prometheus Adapter</title></head>
			<body>
			<h1>Redis Prometheus Adapter</h1>
			</body>
			</html>`))
	})

	http.HandleFunc("/write", ca.handleWrite)
	http.HandleFunc("/read", ca.handleRead)
	http.Handle("/metrics", promhttp.Handler())
	log.With("address", *listenAddress).Info("Listening")
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}
