package csi

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ezgamehost/ezgh-csi/internal/ezghclient/apiclient"
)

func TestUnauthorizedIsUnavailable(t *testing.T) {
	err := APIStatus(&apiclient.Error{Service: "Block Storage", Status: 401, Code: "unauthenticated", Message: "no"}, nil)
	if status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
}
