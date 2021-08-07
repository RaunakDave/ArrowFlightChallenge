package main

import (
	"context"
	"log"
	"time"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/flight"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure(),
		grpc.WithBlock())
	c := flight.NewFlightServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(),
		100*time.Second)
	defer cancel()
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	descr := &flight.FlightDescriptor{
		Type: flight.FlightDescriptor_PATH,
		Path: []string{"test_path"},
	}

	i := 0
	doPT, err := c.DoPut(ctx)
	if err != nil {
		panic(err)
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	pool := memory.NewGoAllocator()
	w := flight.NewRecordWriter(doPT, ipc.WithSchema(schema),
		ipc.WithAllocator(pool))
	w.SetFlightDescriptor(descr)
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	for {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		r := b.NewRecord()
		w.Write(r)
		if err != nil {
			panic(err)
		}
		defer r.Release()
		time.Sleep(1 * time.Second)
		i = i + 1
		if i == 10 {
			break
		}
	}
	doPT.CloseSend()

	info, err := c.GetFlightInfo(ctx, descr)
	if err != nil {
		log.Fatal(err)
	}

	stream, err := c.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		log.Fatal(err)
	}

	rdr, err := flight.NewRecordReader(stream)
	if err != nil {
		log.Fatal(err)
	}
	defer rdr.Release()

	for rdr.Next() {
		rec := rdr.Record()
		defer rec.Release()
		log.Println(rec)
	}

}
