package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	upstreamAddr := flag.String("upstream-addr", "127.0.0.1:19530", "upstream Milvus gRPC address (for dialing from host)")
	downstreamAddr := flag.String("downstream-addr", "127.0.0.1:19500", "downstream Milvus gRPC address (for dialing from host)")
	upstreamURI := flag.String("upstream-uri", "", "upstream Milvus URI for inter-cluster communication (defaults to upstream-addr)")
	downstreamURI := flag.String("downstream-uri", "", "downstream Milvus URI for inter-cluster communication (defaults to downstream-addr)")
	upstreamID := flag.String("upstream-id", "backup-test-upstream", "upstream cluster ID")
	downstreamID := flag.String("downstream-id", "backup-test-downstream", "downstream cluster ID")
	pchannelNum := flag.Int("pchannel-num", 16, "number of pchannels")
	target := flag.String("target", "both", "which instance to configure: upstream, downstream, or both")
	flag.Parse()

	if *upstreamURI == "" {
		*upstreamURI = *upstreamAddr
	}
	if *downstreamURI == "" {
		*downstreamURI = *downstreamAddr
	}

	upstreamPchannels := make([]string, 0, *pchannelNum)
	downstreamPchannels := make([]string, 0, *pchannelNum)
	for i := 0; i < *pchannelNum; i++ {
		upstreamPchannels = append(upstreamPchannels, fmt.Sprintf("%s-rootcoord-dml_%d", *upstreamID, i))
		downstreamPchannels = append(downstreamPchannels, fmt.Sprintf("%s-rootcoord-dml_%d", *downstreamID, i))
	}

	config := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId:       *upstreamID,
				ConnectionParam: &commonpb.ConnectionParam{Uri: *upstreamURI},
				Pchannels:       upstreamPchannels,
			},
			{
				ClusterId:       *downstreamID,
				ConnectionParam: &commonpb.ConnectionParam{Uri: *downstreamURI},
				Pchannels:       downstreamPchannels,
			},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{
				SourceClusterId: *upstreamID,
				TargetClusterId: *downstreamID,
			},
		},
	}

	updateFn := func(addr, name string) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("failed to dial %s (%s): %v", name, addr, err)
		}
		defer conn.Close()

		client := milvuspb.NewMilvusServiceClient(conn)
		resp, err := client.UpdateReplicateConfiguration(ctx, &milvuspb.UpdateReplicateConfigurationRequest{
			ReplicateConfiguration: config,
		})
		if err != nil {
			log.Fatalf("failed to update replicate configuration on %s: %v", name, err)
		}
		if resp.GetErrorCode() != commonpb.ErrorCode_Success {
			log.Fatalf("update replicate configuration on %s returned error: %s", name, resp.GetReason())
		}
		log.Printf("replicate configuration updated on %s successfully", name)
	}

	switch *target {
	case "downstream":
		updateFn(*downstreamAddr, "downstream")
	case "upstream":
		updateFn(*upstreamAddr, "upstream")
	case "both":
		updateFn(*downstreamAddr, "downstream")
		updateFn(*upstreamAddr, "upstream")
	default:
		log.Fatalf("invalid target: %s (must be upstream, downstream, or both)", *target)
	}
}
