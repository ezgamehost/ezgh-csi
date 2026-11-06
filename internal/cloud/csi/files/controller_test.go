package files

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

	"github.com/ezgamehost/ezgh-csi/internal/cloud/config"
	"github.com/ezgamehost/ezgh-csi/internal/ezghclient"
)

func init() {
	pollEvery, createWait = 5*time.Millisecond, 100*time.Millisecond
}

const clusterUUID = "0190c0de-0000-7000-8000-000000000001"

type key string

func (k key) APIKey(context.Context) (string, error) { return string(k), nil }

type request struct {
	Method, Path, Key string
	Body              map[string]any
}

// fakeEZGH serves files-api, compute-api and network-api on one server. deny names a
// "METHOD path" suffix answered 403 for an action.
type fakeEZGH struct {
	mu           sync.Mutex
	reqs         []request
	accessPoints map[string]map[string]any // by slug
	mountTargets []map[string]any
	apStatus     string // what a new access point becomes: available (default), creating, failed
	failNext     int    // new access points that end failed
	deny         map[string]string
}

func newController(t *testing.T) (*Controller, *fakeEZGH) {
	f := &fakeEZGH{accessPoints: map[string]map[string]any{}, deny: map[string]string{}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return &Controller{
		EZGH: ezghclient.NewAt(key("ezgh_test"), "test", func(string) string { return srv.URL }),
		Config: func() (config.Config, error) {
			return config.Config{Cluster: "kcl_test", ClusterUUID: clusterUUID, Org: "org_test", Project: "prj_test", Region: "us-west-1", VPC: "vpc_aaaaaaaaaaaa"}, nil
		},
	}, f
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func apiError(w http.ResponseWriter, code int, errCode, msg string) {
	writeJSON(w, code, map[string]any{"error": map[string]any{"code": errCode, "message": msg}})
}

func op(typ, id string) map[string]any {
	return map[string]any{"id": "op-1", "type": typ, "status": "running", "target": map[string]any{"id": id, "type": "accessPoint"}}
}

func (f *fakeEZGH) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	path := strings.TrimPrefix(r.URL.Path, "/v1/organizations/org_test/projects/prj_test")
	f.reqs = append(f.reqs, request{r.Method, path, r.Header.Get("Idempotency-Key"), body})
	for suffix, action := range f.deny {
		if strings.HasSuffix(r.Method+" "+path, suffix) {
			apiError(w, 403, "access_denied", "This API key isn't allowed to "+action)
			return
		}
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	switch {
	case r.Method == "POST" && path == "/access-points":
		for _, a := range f.accessPoints {
			if a["key"] == r.Header.Get("Idempotency-Key") {
				writeJSON(w, 202, op("CreateAccessPoint", a["id"].(string)))
				return
			}
		}
		n := len(f.accessPoints) + 1
		st := f.apStatus
		if st == "" {
			st = "available"
		}
		if f.failNext > 0 {
			f.failNext--
			st = "failed"
		}
		a := map[string]any{"id": fmt.Sprintf("0190c0de-0000-7000-8000-%012d", n), "slug": fmt.Sprintf("fap_%012d", n),
			"status": st, "fileSystem": body["fileSystem"], "key": r.Header.Get("Idempotency-Key")}
		f.accessPoints[a["slug"].(string)] = a
		writeJSON(w, 202, op("CreateAccessPoint", a["id"].(string)))
	case len(parts) == 2 && parts[0] == "access-points":
		var a map[string]any
		for _, v := range f.accessPoints {
			if v["slug"] == parts[1] || v["id"] == parts[1] {
				a = v
			}
		}
		switch {
		case a == nil:
			apiError(w, 404, "not_found", "Access point not found")
		case r.Method == "DELETE" && a["status"] == "failed":
			a["status"] = "deleting" // kept, so a replay of its create still names it
			writeJSON(w, 202, op("DeleteAccessPoint", a["id"].(string)))
		case r.Method == "DELETE":
			delete(f.accessPoints, a["slug"].(string))
			writeJSON(w, 202, op("DeleteAccessPoint", a["id"].(string)))
		default:
			writeJSON(w, 200, a)
		}
	case r.Method == "GET" && len(parts) == 2 && parts[0] == "file-systems":
		writeJSON(w, 200, map[string]any{"slug": parts[1], "status": "available"})
	case r.Method == "GET" && path == "/mount-targets":
		var out []any
		for _, m := range f.mountTargets {
			if m["fileSystem"] == r.URL.Query().Get("fileSystem") {
				out = append(out, m)
			}
		}
		writeJSON(w, 200, map[string]any{"mountTargets": out, "nextCursor": nil})
	case r.Method == "GET" && path == "/instances/ins_bbbbbbbbbbbb":
		writeJSON(w, 200, map[string]any{"slug": "ins_bbbbbbbbbbbb", "status": "running", "tags": map[string]any{"ezgh:kubernetes-cluster": clusterUUID},
			"networkInterfaceIds": []string{"eni_pub000000000", "eni_prv000000000"}})
	case r.Method == "GET" && path == "/network-interfaces/eni_pub000000000":
		writeJSON(w, 200, map[string]any{"slug": "eni_pub000000000", "attachment": map[string]any{"instance": "ins_bbbbbbbbbbbb", "primary": true}})
	case r.Method == "GET" && path == "/network-interfaces/eni_prv000000000":
		writeJSON(w, 200, map[string]any{"slug": "eni_prv000000000", "vpc": "vpc_aaaaaaaaaaaa", "subnet": "sbn_bbbbbbbbbbbb",
			"attachment": map[string]any{"instance": "ins_bbbbbbbbbbbb", "primary": false}})
	default:
		apiError(w, 500, "unexpected", r.Method+" "+path)
	}
}

func (f *fakeEZGH) requests(method, prefix string) []request {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []request
	for _, r := range f.reqs {
		if r.Method == method && strings.HasPrefix(r.Path, prefix) {
			out = append(out, r)
		}
	}
	return out
}

func mountTarget(fs, subnet, st, host string) map[string]any {
	return map[string]any{"slug": "fmt_" + subnet[4:], "fileSystem": fs, "vpc": "vpc_aaaaaaaaaaaa", "subnet": subnet, "status": st, "hostname": host}
}

func caps(mode csi.VolumeCapability_AccessMode_Mode) []*csi.VolumeCapability {
	return []*csi.VolumeCapability{{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: mode},
	}}
}

var rwx = caps(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER)

func code(err error) codes.Code { return status.Code(err) }

func create(c *Controller, params map[string]string) (*csi.CreateVolumeResponse, error) {
	return c.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "pvc-abc", Parameters: params, VolumeCapabilities: rwx,
		CapacityRange: &csi.CapacityRange{RequiredBytes: 5 << 30},
	})
}

func TestCreateAccessPoint(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params map[string]string
		want   string // the POST body's rootDirectory and posixUser
		tls    bool
	}{
		{"defaults", map[string]string{"fileSystem": "fsy_aaaaaaaaaaaa"},
			`{"creationInfo":{"ownerGid":1000,"ownerUid":1000,"permissions":"0770"},"path":"/kubernetes/pvc-abc"} {"gid":1000,"uid":1000}`, false},
		{"set", map[string]string{"fileSystem": "fsy_aaaaaaaaaaaa", "uid": "33", "gid": "34", "directoryPerms": "0750", "tls": "true",
			"csi.storage.k8s.io/pvc/name": "data", "csi.storage.k8s.io/pvc/namespace": "web", "csi.storage.k8s.io/pv/name": "pvc-abc"},
			`{"creationInfo":{"ownerGid":34,"ownerUid":33,"permissions":"0750"},"path":"/kubernetes/pvc-abc"} {"gid":34,"uid":33}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, f := newController(t)
			resp, err := create(c, tc.params)
			if err != nil {
				t.Fatal(err)
			}
			v := resp.GetVolume()
			if v.GetVolumeId() != "fap_000000000001" || v.GetCapacityBytes() != 5<<30 || (v.GetVolumeContext()["tls"] == "true") != tc.tls {
				t.Fatal(v)
			}
			p := f.requests("POST", "/access-points")
			if len(p) != 1 || !strings.HasPrefix(p[0].Key, "csi-pvc-abc-") || p[0].Body["fileSystem"] != "fsy_aaaaaaaaaaaa" || p[0].Body["name"] != "pvc-abc" {
				t.Fatal(p)
			}
			root, _ := json.Marshal(p[0].Body["rootDirectory"])
			user, _ := json.Marshal(p[0].Body["posixUser"])
			if got := string(root) + " " + string(user); got != tc.want {
				t.Fatal(got)
			}
			tags := p[0].Body["tags"].(map[string]any)
			if tags["ezgh:kubernetes-cluster"] != clusterUUID || (tc.tls && tags["kubernetes.io/created-for/pvc/namespace"] != "web") {
				t.Fatal(tags)
			}
		})
	}
}

func TestCreateIdempotent(t *testing.T) {
	c, f := newController(t)
	params := map[string]string{"fileSystem": "fsy_aaaaaaaaaaaa"}
	a, err1 := create(c, params)
	b, err2 := create(c, params)
	p := f.requests("POST", "/access-points")
	if err1 != nil || err2 != nil || len(p) != 2 || p[0].Key != p[1].Key || a.Volume.VolumeId != b.Volume.VolumeId {
		t.Fatal(err1, err2, p)
	}
}

func TestCreateAbortedUntilAvailable(t *testing.T) {
	c, f := newController(t)
	f.apStatus = "creating"
	if _, err := create(c, map[string]string{"fileSystem": "fsy_aaaaaaaaaaaa"}); code(err) != codes.Aborted {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.accessPoints["fap_000000000001"]["status"] = "available"
	f.mu.Unlock()
	if r, err := create(c, map[string]string{"fileSystem": "fsy_aaaaaaaaaaaa"}); err != nil || r.Volume.VolumeId != "fap_000000000001" {
		t.Fatal(r, err)
	}
}

func TestCreateReplacesFailed(t *testing.T) {
	c, f := newController(t)
	f.failNext = 1
	r, err := create(c, map[string]string{"fileSystem": "fsy_aaaaaaaaaaaa"})
	if err != nil || r.Volume.VolumeId != "fap_000000000002" {
		t.Fatal(r, err)
	}
	// A retry walks the same chain: the failed one (now deleting), then its replacement.
	if r, err := create(c, map[string]string{"fileSystem": "fsy_aaaaaaaaaaaa"}); err != nil || r.Volume.VolumeId != "fap_000000000002" {
		t.Fatal(r, err)
	}
	p := f.requests("POST", "/access-points")
	if len(p) != 4 || p[0].Key != p[2].Key || p[1].Key != p[3].Key || !strings.HasPrefix(p[1].Key, "csi-pvc-abc-0190c0de-0000-7000-8000-000000000001-") {
		t.Fatal(p)
	}
	if d := f.requests("DELETE", "/access-points/fap_000000000001"); len(d) != 1 {
		t.Fatal(d)
	}
}

func TestFileSystemRequired(t *testing.T) {
	for _, params := range []map[string]string{
		{},
		{"fileSystem": "fsy_short"},
		{"fileSystem": "vol_aaaaaaaaaaaa"},
		{"fileSystem": "fsy_aaaaaaaaaaaa", "uid": "-1"},
		{"fileSystem": "fsy_aaaaaaaaaaaa", "gid": "x"},
		{"fileSystem": "fsy_aaaaaaaaaaaa", "directoryPerms": "0999"},
		{"fileSystem": "fsy_aaaaaaaaaaaa", "tls": "yes"},
		{"fileSystem": "fsy_aaaaaaaaaaaa", "type": "gp1"},
	} {
		c, f := newController(t)
		if _, err := create(c, params); code(err) != codes.InvalidArgument {
			t.Fatal(params, err)
		}
		if len(f.requests("POST", "")) != 0 {
			t.Fatal(params, "sent a create")
		}
	}
	c, _ := newController(t)
	_, err := c.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "pvc-abc", Parameters: map[string]string{"fileSystem": "fsy_aaaaaaaaaaaa"},
		VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER}}},
	})
	if code(err) != codes.InvalidArgument {
		t.Fatal("a block volume:", err)
	}
}

func TestStaticHandle(t *testing.T) {
	for _, tc := range []struct {
		handle, fs, ap, sub string
		ok                  bool
	}{
		{"fsy_aaaaaaaaaaaa", "fsy_aaaaaaaaaaaa", "", "", true},
		{"fsy_aaaaaaaaaaaa:/", "fsy_aaaaaaaaaaaa", "", "", true},
		{"fsy_aaaaaaaaaaaa:/shared/web", "fsy_aaaaaaaaaaaa", "", "/shared/web", true},
		{"fap_aaaaaaaaaaaa", "", "fap_aaaaaaaaaaaa", "", true},
		{"fsy_aaaaaaaaaaaa:/a/../../etc", "", "", "", false},
		{"fsy_aaaaaaaaaaaa:/..", "", "", "", false},
		{"fsy_aaaaaaaaaaaa:shared", "", "", "", false},
		{"fsy_aaaaaaaaaaaa:/a//b", "", "", "", false},
		{"fsy_aaaaaaaaaaaa:/a/", "", "", "", false},
		{"fap_aaaaaaaaaaaa:/x", "", "", "", false},
		{"fsy_aaaaaaaaaaaa/x", "", "", "", false},
		{"vol_aaaaaaaaaaaa", "", "", "", false},
		{"", "", "", "", false},
	} {
		h, err := parseHandle(tc.handle)
		if (err == nil) != tc.ok || (tc.ok && (h.fileSystem != tc.fs || h.accessPoint != tc.ap || h.path != tc.sub)) {
			t.Fatal(tc.handle, h, err)
		}
		if err != nil && code(err) != codes.InvalidArgument {
			t.Fatal(tc.handle, err)
		}
	}
}

func publish(c *Controller, volume string) (*csi.ControllerPublishVolumeResponse, error) {
	return c.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: volume, NodeId: "ins_bbbbbbbbbbbb", VolumeCapability: rwx[0],
	})
}

func TestMountTargetPrefersNodeSubnet(t *testing.T) {
	c, f := newController(t)
	f.mountTargets = []map[string]any{
		mountTarget("fsy_aaaaaaaaaaaa", "sbn_aaaaaaaaaaaa", "available", "a.fs.us-west-1.ezghcompute.com"),
		mountTarget("fsy_aaaaaaaaaaaa", "sbn_bbbbbbbbbbbb", "available", "b.fs.us-west-1.ezghcompute.com"),
		mountTarget("fsy_cccccccccccc", "sbn_bbbbbbbbbbbb", "available", "other.fs.us-west-1.ezghcompute.com"),
	}
	r, err := publish(c, "fsy_aaaaaaaaaaaa:/shared")
	if err != nil || r.PublishContext["hostname"] != "b.fs.us-west-1.ezghcompute.com" {
		t.Fatal(r, err)
	}

	// The node's subnet has only a mount target that isn't available: any available one.
	f.mountTargets[1]["status"] = "creating"
	r, err = publish(c, "fsy_aaaaaaaaaaaa")
	if err != nil || r.PublishContext["hostname"] != "a.fs.us-west-1.ezghcompute.com" {
		t.Fatal(r, err)
	}

	// One available mount target: the node isn't read at all.
	f.reqs = nil
	f.accessPoints["fap_aaaaaaaaaaaa"] = map[string]any{"id": "x", "slug": "fap_aaaaaaaaaaaa", "status": "available", "fileSystem": "fsy_aaaaaaaaaaaa"}
	r, err = publish(c, "fap_aaaaaaaaaaaa")
	if err != nil || r.PublishContext["hostname"] != "a.fs.us-west-1.ezghcompute.com" || len(f.requests("GET", "/instances")) != 0 {
		t.Fatal(r, err, f.reqs)
	}
	if _, err := c.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{VolumeId: "fap_aaaaaaaaaaaa", NodeId: "ins_bbbbbbbbbbbb"}); err != nil {
		t.Fatal(err)
	}
}

func TestNoMountTargetEvent(t *testing.T) {
	c, f := newController(t)
	f.mountTargets = []map[string]any{mountTarget("fsy_aaaaaaaaaaaa", "sbn_bbbbbbbbbbbb", "failed", "b.fs.us-west-1.ezghcompute.com")}
	_, err := publish(c, "fsy_aaaaaaaaaaaa")
	if code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "NoMountTarget") || !strings.Contains(err.Error(), "fsy_aaaaaaaaaaaa") {
		t.Fatal(err)
	}
}

func TestDeleteKeepsFiles(t *testing.T) {
	c, f := newController(t)
	f.accessPoints["fap_aaaaaaaaaaaa"] = map[string]any{"id": "x", "slug": "fap_aaaaaaaaaaaa", "status": "available", "fileSystem": "fsy_aaaaaaaaaaaa",
		"tags": map[string]any{"ezgh:kubernetes-cluster": clusterUUID}}
	for _, id := range []string{"fap_aaaaaaaaaaaa", "fap_aaaaaaaaaaaa", "fsy_aaaaaaaaaaaa", "fsy_aaaaaaaaaaaa:/shared"} {
		if _, err := c.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: id}); err != nil {
			t.Fatal(id, err)
		}
	}
	// The access point's DELETE (the second call reads a 404: done), and nothing else.
	if d := f.requests("DELETE", ""); len(d) != 1 || d[0].Path != "/access-points/fap_aaaaaaaaaaaa" || len(f.reqs) != 3 {
		t.Fatal(f.reqs)
	}
}

// A PV copied into another cluster of the project (a restored backup) never deletes the first
// cluster's access point, nor one the customer made.
func TestDeleteOnlyThisClustersAccessPoint(t *testing.T) {
	c, f := newController(t)
	f.accessPoints["fap_aaaaaaaaaaaa"] = map[string]any{"id": "x", "slug": "fap_aaaaaaaaaaaa", "status": "available",
		"tags": map[string]any{"ezgh:kubernetes-cluster": "0190c0de-0000-7000-8000-000000000999"}}
	f.accessPoints["fap_bbbbbbbbbbbb"] = map[string]any{"id": "y", "slug": "fap_bbbbbbbbbbbb", "status": "available", "tags": map[string]any{}}
	for _, id := range []string{"fap_aaaaaaaaaaaa", "fap_bbbbbbbbbbbb"} {
		if _, err := c.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: id}); err != nil {
			t.Fatal(id, err)
		}
	}
	if d := f.requests("DELETE", ""); len(d) != 0 || len(f.accessPoints) != 2 {
		t.Fatal(d)
	}
	f.deny["GET /access-points/fap_aaaaaaaaaaaa"] = "files.accessPoints.get"
	if _, err := c.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "fap_aaaaaaaaaaaa"}); code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
}

func TestPublishAccessPointNotAvailable(t *testing.T) {
	c, f := newController(t)
	f.mountTargets = []map[string]any{mountTarget("fsy_aaaaaaaaaaaa", "sbn_bbbbbbbbbbbb", "available", "b.fs.us-west-1.ezghcompute.com")}
	f.accessPoints["fap_aaaaaaaaaaaa"] = map[string]any{"id": "x", "slug": "fap_aaaaaaaaaaaa", "status": "deleting", "fileSystem": "fsy_aaaaaaaaaaaa"}
	if _, err := publish(c, "fap_aaaaaaaaaaaa"); code(err) != codes.FailedPrecondition {
		t.Fatal(err)
	}
	_, err := c.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: "fsy_aaaaaaaaaaaa", NodeId: "ins_bbbbbbbbbbbb", VolumeCapability: rwx[0], VolumeContext: map[string]string{"tls": "TRUE"},
	})
	if code(err) != codes.InvalidArgument {
		t.Fatal("tls attribute:", err)
	}
}

func TestAccessPointDenied(t *testing.T) {
	c, f := newController(t)
	f.deny["POST /access-points"] = "files.accessPoints.create"
	_, err := create(c, map[string]string{"fileSystem": "fsy_aaaaaaaaaaaa"})
	if code(err) != codes.PermissionDenied || !strings.Contains(err.Error(), "files.accessPoints.create") {
		t.Fatal(err)
	}
	f.deny = map[string]string{"GET /mount-targets": "files.mountTargets.list"}
	if _, err := publish(c, "fsy_aaaaaaaaaaaa"); code(err) != codes.PermissionDenied || !strings.Contains(err.Error(), "files.mountTargets.list") {
		t.Fatal(err)
	}
}
