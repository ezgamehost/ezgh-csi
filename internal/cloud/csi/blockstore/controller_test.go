package blockstore

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/ezgamehost/ezgh-csi/internal/cloud/config"
	"github.com/ezgamehost/ezgh-csi/internal/ezghclient"
)

func init() {
	pollEvery, createWait, attachWait = 5*time.Millisecond, 200*time.Millisecond, 200*time.Millisecond
}

type key string

func (k key) APIKey(context.Context) (string, error) { return string(k), nil }

// request is one call the fake EZGH APIs got.
type request struct {
	Method, Path, Key string
	Body              map[string]any
}

// fakeEZGH serves blockstore-api and compute-api on one server. Volumes and instances live in
// maps; deny names a path suffix answered 403 for an action; answer overrides a path suffix.
type fakeEZGH struct {
	mu            sync.Mutex
	reqs          []request
	volumes       map[string]map[string]any
	snapshots     map[string]map[string]any
	instances     map[string]string // ins_ → status; missing is 404
	answer        map[string]func(w http.ResponseWriter, body map[string]any)
	deny          map[string]string
	failNext      int // new volumes that end failed
	failSnapshots int // new snapshots that end failed
}

func newFake(t *testing.T) (*fakeEZGH, *ezghclient.Clients) {
	f := &fakeEZGH{volumes: map[string]map[string]any{}, snapshots: map[string]map[string]any{}, instances: map[string]string{},
		answer: map[string]func(http.ResponseWriter, map[string]any){}, deny: map[string]string{}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, ezghclient.NewAt(key("ezgh_test"), "test", func(string) string { return srv.URL })
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func apiError(w http.ResponseWriter, code int, errCode, msg string) {
	writeJSON(w, code, map[string]any{"error": map[string]any{"code": errCode, "message": msg}})
}

func (f *fakeEZGH) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	path := strings.TrimPrefix(r.URL.Path, "/v1/organizations/org_test")
	path = strings.TrimPrefix(path, "/projects/prj_test")
	f.reqs = append(f.reqs, request{r.Method, path, r.Header.Get("Idempotency-Key"), body})
	for suffix, action := range f.deny {
		if strings.HasSuffix(r.Method+" "+path, suffix) {
			apiError(w, 403, "access_denied", "This API key isn't allowed to "+action)
			return
		}
	}
	for suffix, fn := range f.answer {
		if strings.HasSuffix(r.Method+" "+path, suffix) {
			fn(w, body)
			return
		}
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	op := func(typ string, extra map[string]any) map[string]any {
		o := map[string]any{"id": "op-1", "type": typ, "status": "succeeded"}
		for k, v := range extra {
			o[k] = v
		}
		return o
	}
	switch {
	case r.Method == "GET" && path == "/volume-types":
		writeJSON(w, 200, map[string]any{"volumeTypes": []any{
			map[string]any{"name": "gp1", "sizeGiB": map[string]any{"min": 1, "max": 16384}},
			map[string]any{"name": "hp1", "sizeGiB": map[string]any{"min": 4, "max": 16384}},
		}})
	case r.Method == "POST" && path == "/volumes":
		// Idempotent by key, as the service is.
		for _, v := range f.volumes {
			if v["key"] == r.Header.Get("Idempotency-Key") {
				writeJSON(w, 202, op("CreateVolume", map[string]any{"volume": v}))
				return
			}
		}
		id := fmt.Sprintf("vol_%012d", len(f.volumes)+1)
		st := "available"
		if f.failNext > 0 {
			f.failNext--
			st = "failed"
		}
		v := map[string]any{"slug": id, "status": st, "sizeBytes": body["sizeBytes"], "volumeType": body["volumeType"], "key": r.Header.Get("Idempotency-Key")}
		f.volumes[id] = v
		writeJSON(w, 202, op("CreateVolume", map[string]any{"volume": v}))
	case len(parts) == 2 && parts[0] == "volumes":
		v, ok := f.volumes[parts[1]]
		switch {
		case !ok:
			apiError(w, 404, "not_found", "no such volume")
		case r.Method == "DELETE" && v["attachment"] != nil:
			apiError(w, 409, "volume_in_use", "Volume is attached")
		case r.Method == "DELETE" && v["status"] == "failed":
			v["status"] = "deleting" // kept, so a replay of its create still names it
			writeJSON(w, 202, op("DeleteVolume", nil))
		case r.Method == "DELETE":
			delete(f.volumes, parts[1])
			writeJSON(w, 202, op("DeleteVolume", nil))
		default:
			writeJSON(w, 200, v)
		}
	case len(parts) == 3 && parts[0] == "volumes" && parts[2] == "modify":
		f.volumes[parts[1]]["sizeBytes"] = body["sizeBytes"]
		writeJSON(w, 202, op("ModifyVolume", map[string]any{"volume": f.volumes[parts[1]]}))
	case r.Method == "POST" && path == "/snapshots":
		for _, s := range f.snapshots { // idempotent by key, as the service is
			if s["key"] != nil && s["key"] == r.Header.Get("Idempotency-Key") {
				writeJSON(w, 202, op("CreateSnapshot", map[string]any{"snapshot": s}))
				return
			}
		}
		id := fmt.Sprintf("snp_%012d", len(f.snapshots)+1)
		st := "creating"
		if f.failSnapshots > 0 {
			f.failSnapshots--
			st = "failed"
		}
		f.snapshots[id] = map[string]any{"slug": id, "status": st, "key": r.Header.Get("Idempotency-Key"), "volume": body["volumeId"], "volumeSizeBytes": "1073741824", "createdAt": "2026-11-06T10:00:00.000Z"}
		writeJSON(w, 202, op("CreateSnapshot", map[string]any{"snapshot": f.snapshots[id]}))
	case len(parts) == 2 && parts[0] == "snapshots":
		if s, ok := f.snapshots[parts[1]]; ok && r.Method == "DELETE" && s["status"] == "failed" {
			s["status"] = "deleting" // kept, so a replay of its create still names it
			writeJSON(w, 202, op("DeleteSnapshot", nil))
		} else if ok && r.Method == "DELETE" {
			delete(f.snapshots, parts[1])
			writeJSON(w, 202, op("DeleteSnapshot", nil))
		} else if ok {
			writeJSON(w, 200, s)
		} else {
			apiError(w, 404, "not_found", "no such snapshot")
		}
	case len(parts) == 2 && parts[0] == "instances":
		if st, ok := f.instances[parts[1]]; ok {
			writeJSON(w, 200, map[string]any{"slug": parts[1], "status": st})
		} else {
			apiError(w, 404, "not_found", "no such instance")
		}
	case len(parts) == 3 && parts[0] == "instances":
		writeJSON(w, 200, map[string]any{"slug": parts[1], "status": "running"})
	case len(parts) == 2 && parts[0] == "operations":
		writeJSON(w, 200, op("AttachVolume", nil))
	default:
		apiError(w, 500, "unexpected", r.Method+" "+path)
	}
}

func (f *fakeEZGH) posts(suffix string) []request {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []request
	for _, r := range f.reqs {
		if r.Method == "POST" && strings.HasSuffix(r.Path, suffix) {
			out = append(out, r)
		}
	}
	return out
}

func newController(t *testing.T, objs ...runtime.Object) (*Controller, *fakeEZGH) {
	f, c := newFake(t)
	return &Controller{
		Blockstore: c.Blockstore, Compute: c.Compute, Nodes: fake.NewClientset(objs...),
		Config: func() (config.Config, error) {
			return config.Config{Cluster: "kcl_test", ClusterUUID: "0190c0de-0000-7000-8000-000000000001", Org: "org_test", Project: "prj_test", Region: "us-west-1"}, nil
		},
	}, f
}

func mountCap(fs string) []*csi.VolumeCapability {
	return []*csi.VolumeCapability{{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: fs}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	}}
}

func code(err error) codes.Code { return status.Code(err) }

func TestCreateVolumeParameters(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params map[string]string
		fs     string
		bytes  int64
		want   codes.Code
		check  func(t *testing.T, body map[string]any)
	}{
		{name: "default type, rounded up to GiB", bytes: 1<<30 + 1, check: func(t *testing.T, b map[string]any) {
			if b["volumeType"] != "gp1" || b["sizeBytes"] != "2147483648" {
				t.Fatal(b)
			}
		}},
		{name: "xfs", fs: "xfs", bytes: 1 << 30},
		{name: "iops and the type's minimum", params: map[string]string{"type": "hp1", "iops": "6000", "throughputMBps": "250"}, bytes: 1 << 30,
			check: func(t *testing.T, b map[string]any) {
				if b["volumeType"] != "hp1" || b["iops"] != float64(6000) || b["throughputMBps"] != float64(250) || b["sizeBytes"] != "4294967296" {
					t.Fatal(b)
				}
			}},
		{name: "tags", params: map[string]string{"csi.storage.k8s.io/pvc/name": "data", "csi.storage.k8s.io/pvc/namespace": "app", "csi.storage.k8s.io/pv/name": "pvc-1"}, bytes: 1 << 30,
			check: func(t *testing.T, b map[string]any) {
				tags := b["tags"].(map[string]any)
				if tags["ezgh:kubernetes-cluster"] != "0190c0de-0000-7000-8000-000000000001" || tags["kubernetes.io/created-for/pvc/name"] != "data" ||
					tags["kubernetes.io/created-for/pvc/namespace"] != "app" || tags["kubernetes.io/created-for/pv/name"] != "pvc-1" || len(tags) != 4 {
					t.Fatal(tags)
				}
			}},
		{name: "btrfs", fs: "btrfs", want: codes.InvalidArgument},
		{name: "unknown parameter", params: map[string]string{"size": "10"}, want: codes.InvalidArgument},
		{name: "unknown type", params: map[string]string{"type": "nope"}, want: codes.InvalidArgument},
		{name: "bad iops", params: map[string]string{"iops": "lots"}, want: codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, f := newController(t)
			_, err := c.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Name: "pvc-1", Parameters: tc.params, VolumeCapabilities: mountCap(tc.fs), CapacityRange: &csi.CapacityRange{RequiredBytes: tc.bytes},
			})
			if code(err) != tc.want {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			if tc.check != nil {
				tc.check(t, f.posts("/volumes")[0].Body)
			}
		})
	}
}

func TestCreateVolumeIdempotent(t *testing.T) {
	c, f := newController(t)
	req := &csi.CreateVolumeRequest{Name: "pvc-abc", VolumeCapabilities: mountCap(""), CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30}}
	a, err := c.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	posts := f.posts("/volumes")
	if len(posts) != 2 || posts[0].Key != posts[1].Key || !strings.HasPrefix(posts[0].Key, "csi-pvc-abc-") || a.Volume.VolumeId != b.Volume.VolumeId {
		t.Fatal(posts, a, b)
	}
}

func TestCreateVolumeWaitsForAvailable(t *testing.T) {
	c, f := newController(t)
	f.answer["GET /volumes/vol_000000000001"] = func(w http.ResponseWriter, _ map[string]any) {
		writeJSON(w, 200, map[string]any{"slug": "vol_000000000001", "status": "creating", "sizeBytes": "1073741824"})
	}
	_, err := c.CreateVolume(context.Background(), &csi.CreateVolumeRequest{Name: "pvc-1", VolumeCapabilities: mountCap("")})
	if code(err) != codes.Aborted {
		t.Fatal(err)
	}
}

func TestCreateFromSnapshot(t *testing.T) {
	c, f := newController(t)
	f.snapshots["snp_aaaaaaaaaaaa"] = map[string]any{"slug": "snp_aaaaaaaaaaaa", "status": "available", "volumeType": "hp1", "volumeSizeBytes": "8589934592"}
	src := &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "snp_aaaaaaaaaaaa"}}}
	resp, err := c.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "pvc-1", VolumeCapabilities: mountCap(""), CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30}, VolumeContentSource: src,
	})
	if err != nil {
		t.Fatal(err)
	}
	body := f.posts("/volumes")[0].Body
	source := body["source"].(map[string]any)
	if source["kind"] != "snapshot" || source["snapshotId"] != "snp_aaaaaaaaaaaa" || body["volumeType"] != "hp1" || body["sizeBytes"] != "8589934592" {
		t.Fatal(body)
	}
	if resp.Volume.ContentSource.GetSnapshot().GetSnapshotId() != "snp_aaaaaaaaaaaa" || resp.Volume.CapacityBytes != 8<<30 {
		t.Fatal(resp)
	}
	clone := &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "vol_x"}}}
	if _, err := c.CreateVolume(context.Background(), &csi.CreateVolumeRequest{Name: "pvc-2", VolumeCapabilities: mountCap(""), VolumeContentSource: clone}); code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
}

func publish(c *Controller) error {
	_, err := c.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: "vol_aaaaaaaaaaaa", NodeId: "ins_bbbbbbbbbbbb", VolumeCapability: mountCap("")[0]})
	return err
}

func TestPublish200And202(t *testing.T) {
	c, f := newController(t)
	if err := publish(c); err != nil {
		t.Fatal(err)
	}
	body := f.posts("/instances/ins_bbbbbbbbbbbb/attach-volume")[0].Body
	if body["volumeId"] != "vol_aaaaaaaaaaaa" || body["deleteOnTermination"] != nil {
		t.Fatal(body)
	}
	polls := 0
	f.answer["POST /instances/ins_bbbbbbbbbbbb/attach-volume"] = func(w http.ResponseWriter, _ map[string]any) {
		writeJSON(w, 202, map[string]any{"id": "op-9", "type": "AttachVolume", "status": "running"})
	}
	f.answer["GET /operations/op-9"] = func(w http.ResponseWriter, _ map[string]any) {
		polls++
		st := "running"
		if polls == 3 {
			st = "succeeded"
		}
		writeJSON(w, 200, map[string]any{"id": "op-9", "type": "AttachVolume", "status": st})
	}
	if err := publish(c); err != nil || polls != 3 {
		t.Fatal(err, polls)
	}
	// Still running past the wait: Aborted, for the attacher to retry.
	f.answer["GET /operations/op-9"] = func(w http.ResponseWriter, _ map[string]any) {
		writeJSON(w, 200, map[string]any{"id": "op-9", "type": "AttachVolume", "status": "running"})
	}
	if err := publish(c); code(err) != codes.Aborted {
		t.Fatal(err)
	}
}

func TestPublishVolumeLimit(t *testing.T) {
	c, f := newController(t)
	for errCode, want := range map[string]codes.Code{
		"volume_limit_exceeded": codes.ResourceExhausted, "instance_restart_required": codes.FailedPrecondition, "volume_detaching": codes.Aborted,
	} {
		f.answer["POST /instances/ins_bbbbbbbbbbbb/attach-volume"] = func(w http.ResponseWriter, _ map[string]any) {
			apiError(w, 409, errCode, "compute says "+errCode)
		}
		if err := publish(c); code(err) != want || status.Convert(err).Message() != "compute says "+errCode {
			t.Fatal(errCode, err)
		}
	}
}

// Review Focus 4: a Node deleted while its instance runs detaches normally, never by force.
func TestForceOnlyWhenNodeGoneAndInstanceTerminated(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "ins-bbbbbbbbbbbb"}}
	for _, tc := range []struct {
		name     string
		node     bool
		instance string // "" is 404
		force    bool
	}{
		{"node present, instance terminated", true, "terminated", false},
		{"node gone, instance running", false, "running", false},
		{"node gone, instance terminated", false, "terminated", true},
		{"node gone, instance 404", false, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var objs []runtime.Object
			if tc.node {
				objs = append(objs, node)
			}
			c, f := newController(t, objs...)
			f.volumes["vol_aaaaaaaaaaaa"] = map[string]any{"slug": "vol_aaaaaaaaaaaa", "status": "in-use", "attachment": map[string]any{"instance": "ins_bbbbbbbbbbbb"}}
			if tc.instance != "" {
				f.instances["ins_bbbbbbbbbbbb"] = tc.instance
			}
			listed := 0
			f.answer["GET /instances"] = func(w http.ResponseWriter, _ map[string]any) {
				listed++
				writeJSON(w, 200, map[string]any{"instances": []any{}, "nextCursor": nil})
			}
			_, err := c.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{VolumeId: "vol_aaaaaaaaaaaa", NodeId: "ins_bbbbbbbbbbbb"})
			if err != nil {
				t.Fatal(err)
			}
			posts := f.posts("/detach-volume")
			if len(posts) != 1 || (posts[0].Body["force"] == true) != tc.force {
				t.Fatal(posts)
			}
			if tc.instance == "" && listed != 1 {
				t.Fatal("a 404 forces only after the project answers a list")
			}
		})
	}
}

func TestUnpublishAlreadyDetached(t *testing.T) {
	c, f := newController(t)
	f.volumes["vol_aaaaaaaaaaaa"] = map[string]any{"slug": "vol_aaaaaaaaaaaa", "status": "available"}
	f.volumes["vol_cccccccccccc"] = map[string]any{"slug": "vol_cccccccccccc", "status": "in-use", "attachment": map[string]any{"instance": "ins_other0000000"}}
	for _, vol := range []string{"vol_aaaaaaaaaaaa", "vol_cccccccccccc", "vol_gone00000000"} {
		if _, err := c.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{VolumeId: vol, NodeId: "ins_bbbbbbbbbbbb"}); err != nil {
			t.Fatal(vol, err)
		}
	}
	if len(f.posts("/detach-volume")) != 0 {
		t.Fatal("detached a volume that wasn't attached to the node")
	}
}

func TestExpandOnline(t *testing.T) {
	c, f := newController(t)
	f.volumes["vol_aaaaaaaaaaaa"] = map[string]any{"slug": "vol_aaaaaaaaaaaa", "status": "in-use", "sizeBytes": "1073741824"}
	resp, err := c.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId: "vol_aaaaaaaaaaaa", CapacityRange: &csi.CapacityRange{RequiredBytes: 3<<30 - 5}, VolumeCapability: mountCap("")[0]})
	if err != nil {
		t.Fatal(err)
	}
	if body := f.posts("/modify")[0].Body; body["sizeBytes"] != "3221225472" || resp.CapacityBytes != 3<<30 || !resp.NodeExpansionRequired {
		t.Fatal(body, resp)
	}
	// Already that size: nothing to modify.
	if _, err := c.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId: "vol_aaaaaaaaaaaa", CapacityRange: &csi.CapacityRange{RequiredBytes: 3 << 30}}); err != nil || len(f.posts("/modify")) != 1 {
		t.Fatal(err)
	}
}

func TestDeleteInUse(t *testing.T) {
	c, f := newController(t)
	f.volumes["vol_aaaaaaaaaaaa"] = map[string]any{"slug": "vol_aaaaaaaaaaaa", "status": "in-use", "attachment": map[string]any{"instance": "ins_bbbbbbbbbbbb"}, "tags": ours}
	if _, err := c.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "vol_aaaaaaaaaaaa"}); code(err) != codes.FailedPrecondition {
		t.Fatal(err)
	}
	if _, err := c.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "vol_gone00000000"}); err != nil {
		t.Fatal("a 404 is a deleted volume:", err)
	}
}

var ours = map[string]any{"ezgh:kubernetes-cluster": "0190c0de-0000-7000-8000-000000000001"}

// A PV or VolumeSnapshotContent copied into another cluster of the project (a restored backup)
// never deletes the first cluster's volume or snapshot.
func TestDeleteOnlyThisClusters(t *testing.T) {
	c, f := newController(t)
	other := map[string]any{"ezgh:kubernetes-cluster": "0190c0de-0000-7000-8000-000000000999"}
	f.volumes["vol_aaaaaaaaaaaa"] = map[string]any{"slug": "vol_aaaaaaaaaaaa", "status": "available", "tags": other}
	f.volumes["vol_bbbbbbbbbbbb"] = map[string]any{"slug": "vol_bbbbbbbbbbbb", "status": "available"}
	f.volumes["vol_cccccccccccc"] = map[string]any{"slug": "vol_cccccccccccc", "status": "available", "tags": ours}
	f.snapshots["snp_aaaaaaaaaaaa"] = map[string]any{"slug": "snp_aaaaaaaaaaaa", "status": "available", "tags": other}
	f.snapshots["snp_cccccccccccc"] = map[string]any{"slug": "snp_cccccccccccc", "status": "available", "tags": ours}
	for _, id := range []string{"vol_aaaaaaaaaaaa", "vol_bbbbbbbbbbbb", "vol_cccccccccccc"} {
		if _, err := c.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: id}); err != nil {
			t.Fatal(id, err)
		}
	}
	for _, id := range []string{"snp_aaaaaaaaaaaa", "snp_cccccccccccc"} {
		if _, err := c.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{SnapshotId: id}); err != nil {
			t.Fatal(id, err)
		}
	}
	var deletes []string
	for _, r := range f.reqs {
		if r.Method == "DELETE" {
			deletes = append(deletes, r.Path)
		}
	}
	if strings.Join(deletes, " ") != "/volumes/vol_cccccccccccc /snapshots/snp_cccccccccccc" {
		t.Fatal(deletes)
	}
	f.deny["GET /volumes/vol_aaaaaaaaaaaa"] = "blockstore.volumes.get"
	if _, err := c.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "vol_aaaaaaaaaaaa"}); code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
}

func TestSnapshot(t *testing.T) {
	c, f := newController(t)
	req := &csi.CreateSnapshotRequest{Name: "snapshot-1", SourceVolumeId: "vol_aaaaaaaaaaaa"}
	resp, err := c.CreateSnapshot(context.Background(), req)
	if err != nil || resp.Snapshot.ReadyToUse || resp.Snapshot.SnapshotId != "snp_000000000001" || resp.Snapshot.SizeBytes != 1<<30 {
		t.Fatal(resp, err)
	}
	f.snapshots["snp_000000000001"]["status"] = "available"
	f.answer["POST /snapshots"] = func(w http.ResponseWriter, _ map[string]any) { // the replay
		writeJSON(w, 202, map[string]any{"id": "op-1", "status": "succeeded", "snapshot": map[string]any{"slug": "snp_000000000001", "status": "creating"}})
	}
	resp, err = c.CreateSnapshot(context.Background(), req)
	if err != nil || !resp.Snapshot.ReadyToUse {
		t.Fatal(resp, err)
	}
	if p := f.posts("/snapshots"); p[0].Key != p[1].Key || !strings.HasPrefix(p[0].Key, "csi-snapshot-1-") {
		t.Fatal(p)
	}
	if _, err := c.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{SnapshotId: "snp_gone00000000"}); err != nil {
		t.Fatal(err)
	}
}

// A failed snapshot is deleted and replaced under a key chained to it; a deleting one is skipped.
func TestSnapshotReplacesFailed(t *testing.T) {
	c, f := newController(t)
	f.failSnapshots = 1
	req := &csi.CreateSnapshotRequest{Name: "snapshot-1", SourceVolumeId: "vol_aaaaaaaaaaaa"}
	r, err := c.CreateSnapshot(context.Background(), req)
	if err != nil || r.Snapshot.SnapshotId != "snp_000000000002" || r.Snapshot.ReadyToUse {
		t.Fatal(r, err)
	}
	if p := f.posts("/snapshots"); len(p) != 2 || !strings.HasPrefix(p[1].Key, "csi-snapshot-1-snp_000000000001") {
		t.Fatal(p)
	}
	if f.snapshots["snp_000000000001"]["status"] != "deleting" {
		t.Fatal(f.snapshots)
	}
	// A retry walks the same chain: the deleting first link moves on to the second.
	r, err = c.CreateSnapshot(context.Background(), req)
	if err != nil || r.Snapshot.SnapshotId != "snp_000000000002" || len(f.snapshots) != 2 {
		t.Fatal(r, err, f.snapshots)
	}
	// Every link failing ends the chain with an error, each failed link deleted.
	c, f = newController(t)
	f.failSnapshots = createLinks
	if _, err := c.CreateSnapshot(context.Background(), req); code(err) != codes.Internal || len(f.snapshots) != createLinks {
		t.Fatal(err, len(f.snapshots))
	}
	for id, s := range f.snapshots {
		if s["status"] != "deleting" {
			t.Fatal(id, s)
		}
	}
}

// Review Focus 1: a narrowed Bot is PermissionDenied naming the action, which the provisioner
// shows on the PVC.
func TestCreateDeniedIsPermissionDenied(t *testing.T) {
	c, f := newController(t)
	f.deny["POST /volumes"] = "blockstore.volumes.create"
	_, err := c.CreateVolume(context.Background(), &csi.CreateVolumeRequest{Name: "pvc-1", VolumeCapabilities: mountCap("")})
	if code(err) != codes.PermissionDenied || !strings.Contains(status.Convert(err).Message(), "blockstore.volumes.create") {
		t.Fatal(err)
	}
}

// A failed volume is deleted and replaced under a key chained to it, and a retry walks the same
// chain: no duplicate, no leak.
func TestCreateVolumeReplacesFailed(t *testing.T) {
	c, f := newController(t)
	f.failNext = 1
	req := &csi.CreateVolumeRequest{Name: "pvc-abc", VolumeCapabilities: mountCap(""), CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30}}
	a, err := c.CreateVolume(context.Background(), req)
	if err != nil || a.Volume.VolumeId != "vol_000000000002" {
		t.Fatal(a, err)
	}
	b, err := c.CreateVolume(context.Background(), req)
	if err != nil || b.Volume.VolumeId != "vol_000000000002" || len(f.volumes) != 2 || f.volumes["vol_000000000001"]["status"] != "deleting" {
		t.Fatal(b, err, f.volumes)
	}
	p := f.posts("/volumes")
	if len(p) != 4 || p[0].Key != p[2].Key || p[1].Key != p[3].Key || !strings.HasPrefix(p[1].Key, "csi-pvc-abc-vol_000000000001-") {
		t.Fatal(p)
	}
	// Every link failing ends the chain with an error.
	c, f = newController(t)
	f.failNext = createLinks
	if _, err := c.CreateVolume(context.Background(), req); code(err) != codes.Internal || len(f.volumes) != createLinks {
		t.Fatal(err, len(f.volumes))
	}
}

func TestCreateIdempotencyConflicts(t *testing.T) {
	for errCode, want := range map[string]codes.Code{"idempotency_conflict": codes.AlreadyExists, "idempotency_in_progress": codes.Aborted} {
		c, f := newController(t)
		f.answer["POST /volumes"] = func(w http.ResponseWriter, _ map[string]any) { apiError(w, 409, errCode, errCode) }
		f.answer["POST /snapshots"] = f.answer["POST /volumes"]
		if _, err := c.CreateVolume(context.Background(), &csi.CreateVolumeRequest{Name: "pvc-1", VolumeCapabilities: mountCap("")}); code(err) != want {
			t.Fatal(errCode, err)
		}
		if _, err := c.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{Name: "s", SourceVolumeId: "vol_aaaaaaaaaaaa"}); code(err) != want {
			t.Fatal(errCode, err)
		}
	}
}

// A 404 from a project that doesn't answer (a wrong project in cloud.json) never forces a detach.
func TestNoForceWhenProjectDoesntAnswer(t *testing.T) {
	c, f := newController(t)
	f.volumes["vol_aaaaaaaaaaaa"] = map[string]any{"slug": "vol_aaaaaaaaaaaa", "status": "in-use", "attachment": map[string]any{"instance": "ins_bbbbbbbbbbbb"}}
	f.answer["GET /instances"] = func(w http.ResponseWriter, _ map[string]any) {
		writeJSON(w, 404, map[string]any{"error": map[string]any{"code": "not_found", "message": "no such project"}})
	}
	if _, err := c.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{VolumeId: "vol_aaaaaaaaaaaa", NodeId: "ins_bbbbbbbbbbbb"}); err == nil {
		t.Fatal("want an error")
	}
	if posts := f.posts("/detach-volume"); len(posts) != 0 {
		t.Fatal(posts)
	}
}
