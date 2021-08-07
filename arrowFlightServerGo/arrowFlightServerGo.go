package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"syscall"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/flight"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Data struct {
	schema    *arrow.Schema
	partition []array.Record
}

type FlightServer struct {
	port         int
	path         string
	partitionMap map[string]Data
}

func (s *FlightServer) CreateServer(port int) flight.Server {
	s.port = port
	s.partitionMap = make(map[string]Data)
	srv := flight.NewServerWithMiddleware(nil, nil)
	srv.RegisterFlightService(&flight.FlightServiceService{
		GetFlightInfo: s.GetFlightInfo,
		DoGet:         s.DoGet,
		DoPut:         s.DoPut,
	})
	return srv
}

func (s *FlightServer) GetFlightInfo(ctx context.Context, in *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if in.Type == flight.FlightDescriptor_PATH {
		if len(in.Path) == 0 {
			return nil, status.Error(codes.InvalidArgument, "invalid path")
		}

		data, ok := s.partitionMap[in.Path[0]]
		if !ok {
			return nil, status.Errorf(codes.NotFound, "flight not found: %s", in.Path[0])
		}

		flightData := &flight.FlightInfo{
			Schema:           flight.SerializeSchema(data.schema, memory.DefaultAllocator),
			FlightDescriptor: in,
			Endpoint: []*flight.FlightEndpoint{{
				Ticket:   &flight.Ticket{Ticket: []byte(in.Path[0])},
				Location: []*flight.Location{{Uri: fmt.Sprintf("localhost:%d", s.port)}},
			}},
			TotalRecords: 0,
			TotalBytes:   -1,
		}
		for _, r := range data.partition {
			flightData.TotalRecords += r.NumRows()
		}
		return flightData, nil
	}
	return nil, status.Error(codes.Unimplemented, in.Type.String())
}

func (s *FlightServer) DoGet(tkt *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	data, ok := s.partitionMap[string(tkt.Ticket)]
	if !ok {
		return status.Errorf(codes.NotFound, "flight not found: %s", string(tkt.Ticket))
	}

	wr := flight.NewRecordWriter(stream, ipc.WithSchema(data.schema))
	defer wr.Close()
	for i, rec := range data.partition {
		wr.WriteWithAppMetadata(rec, []byte(strconv.Itoa(i)))
	}

	return nil
}

func (s *FlightServer) DoPut(stream flight.FlightService_DoPutServer) error {
	rdr, err := flight.NewRecordReader(stream)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	var (
		key     string
		dataset Data
	)

	desc := rdr.LatestFlightDescriptor()
	if desc.Type != flight.FlightDescriptor_PATH || len(desc.Path) < 1 {
		return status.Error(codes.InvalidArgument, "please specify a path")
	}

	key = desc.Path[0]
	dataset.schema = rdr.Schema()
	dataset.partition = make([]array.Record, 0)
	for rdr.Next() {
		rec := rdr.Record()
		rec.Retain()

		dataset.partition = append(dataset.partition, rec)
		if len(rdr.LatestAppMetadata()) > 0 {
			stream.Send(&flight.PutResult{AppMetadata: rdr.LatestAppMetadata()})
		}
	}
	s.partitionMap[key] = dataset
	return nil
}
func main() {
	server := &FlightServer{path: "test"}
	flightserver := server.CreateServer(50051)
	flightserver.Init(fmt.Sprintf("localhost:%d", 50051))
	flightserver.SetShutdownOnSignals(syscall.SIGTERM, os.Interrupt)
	_, p, _ := net.SplitHostPort(flightserver.Addr().String())
	fmt.Printf("Server listening on localhost:%s\n", p)
	flightserver.Serve()
}
