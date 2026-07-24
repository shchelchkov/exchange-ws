package mexc

import (
	"testing"

	mexcpb "exchange-ws/client/mexc/mexcpb"

	"google.golang.org/protobuf/proto"
)

func TestParsePartialBookDepthStreams(t *testing.T) {
	want := &mexcpb.PushDataV3ApiWrapper{
		Channel:  "spot@public.limit.depth.v3.api.pb@SOLUSDT@5",
		Symbol:   proto.String("SOLUSDT"),
		SendTime: proto.Int64(9949473460),
		Body: &mexcpb.PushDataV3ApiWrapper_PublicLimitDepths{
			PublicLimitDepths: &mexcpb.PublicLimitDepthsV3Api{
				Version: "9949473460",
				Asks: []*mexcpb.PublicLimitDepthV3ApiItem{
					{Price: "82.38", Quantity: "670.484"},
					{Price: "82.39", Quantity: "1551.790"},
				},
				Bids: []*mexcpb.PublicLimitDepthV3ApiItem{
					{Price: "82.37", Quantity: "1695.840"},
				},
			},
		},
	}

	raw, err := proto.Marshal(want)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}

	d, ok := ParsePartialBookDepthStreams(string(raw), "sc", "cc")
	if !ok {
		t.Fatal("expected ok=true for valid limit-depth frame")
	}
	m := *d

	if m["symbol"] != "SOLUSDT" {
		t.Errorf("symbol = %v, want SOLUSDT", m["symbol"])
	}
	if m["topic"] != "spot@public.limit.depth.v3.api.pb@SOLUSDT@5" {
		t.Errorf("topic = %v", m["topic"])
	}
	if m["version"] != "9949473460" {
		t.Errorf("version = %v", m["version"])
	}

	asks := m["asks"].([][2]string)
	if len(asks) != 2 || asks[0] != [2]string{"82.38", "670.484"} {
		t.Errorf("asks = %v", asks)
	}
	bids := m["bids"].([][2]string)
	if len(bids) != 1 || bids[0] != [2]string{"82.37", "1695.840"} {
		t.Errorf("bids = %v", bids)
	}
	if _, hasKafkaKey := m["topic"].(string); !hasKafkaKey {
		t.Error("topic must be a string for the Kafka publish key")
	}
}

func TestParsePartialBookDepthStreams_RejectsNonProtobuf(t *testing.T) {
	if _, ok := ParsePartialBookDepthStreams(`{"id":0,"code":0,"msg":"PONG"}`, "sc", "cc"); ok {
		t.Error("expected ok=false for non-protobuf text frame")
	}
}
