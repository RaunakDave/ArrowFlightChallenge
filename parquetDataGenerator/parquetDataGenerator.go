package main

import (
	"log"
	"math/rand"
	"os"
	"runtime"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

type Invoice struct {
	Account  int64   `parquet:"name=account, type=INT64"`
	Product  int64   `parquet:"name=product, type=INT64"`
	Quantity int32   `parquet:"name=quantity, type=INT32"`
	Price    float32 `parquet:"name=price, type=FLOAT"`
	Date     int32   `parquet:"name=date, type=INT32, convertedtype=TIMESTAMP, logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS"`
}

func randomTimestamp() int64 {
	randomTime := rand.Int63n(time.Now().Unix()-94608000) + 94608000

	return randomTime
}

func main() {
	var err error
	emptyFile, err := os.Create("parquet.file")
	if err != nil {
		log.Fatal(err)
	}
	emptyFile.Close()
	fw, err := local.NewLocalFileWriter("parquet.file")
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}

	//write
	pw, err := writer.NewParquetWriter(fw, new(Invoice), int64(runtime.NumCPU()))
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}

	pw.RowGroupSize = 128 * 1024 * 1024
	pw.PageSize = 8 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	num := 100
	for i := 0; i < num; i++ {
		stu := Invoice{
			Product:  int64(20 + i%5),
			Account:  int64(i),
			Quantity: int32(50 + i*5),
			Price:    float32(30.0 + float32(i)*0.1),
			Date:     int32(randomTimestamp()),
		}
		if err = pw.Write(stu); err != nil {
			log.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}
	log.Println("Write Finished")
	fw.Close()

	///read
	fr, err := local.NewLocalFileReader("parquet.file")
	if err != nil {
		log.Println("Can't open file")
		return
	}

	pr, err := reader.NewParquetReader(fr, new(Invoice), int64(runtime.NumCPU()))
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	num = int(pr.GetNumRows())
	for i := 0; i < num/10; i++ {
		if i%2 == 0 {
			pr.SkipRows(10) //skip 10 rows
			continue
		}
		invs := make([]Invoice, 10) //read 10 rows
		if err = pr.Read(&invs); err != nil {
			log.Println("Read error", err)
		}
		log.Println(invs)
	}

	pr.ReadStop()
	fr.Close()
}
