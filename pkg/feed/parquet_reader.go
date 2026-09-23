package feed

import (
	"fmt"
	"io"
	"os"

	"github.com/parquet-go/parquet-go"
)

type parquetTradeRow struct {
	Ticker       string  `parquet:"ticker"`
	SIPTimestamp int64   `parquet:"sip_timestamp"`
	Price        float64 `parquet:"price"`
	Size         float64 `parquet:"size"`
}

type parquetQuoteRow struct {
	Ticker       string  `parquet:"ticker"`
	SIPTimestamp int64   `parquet:"sip_timestamp"`
	BidPrice     float64 `parquet:"bid_price"`
	BidSize      float64 `parquet:"bid_size"`
	AskPrice     float64 `parquet:"ask_price"`
	AskSize      float64 `parquet:"ask_size"`
}

// ParquetTradeReader reads trade ticks from a Parquet file.
type ParquetTradeReader struct {
	symbol  string
	file    *os.File
	reader  *parquet.GenericReader[parquetTradeRow]
	buffer  []parquetTradeRow
	bufIdx  int
	bufLen  int
	current Tick
	err     error
	closed  bool
}

func OpenTradeReader(filePath string, symbol string, batchSize int) (*ParquetTradeReader, error) {
	if batchSize <= 0 {
		batchSize = 2048
	}

	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open trades parquet %s: %w", filePath, err)
	}

	reader := parquet.NewGenericReader[parquetTradeRow](f)
	return &ParquetTradeReader{
		symbol: symbol,
		file:   f,
		reader: reader,
		buffer: make([]parquetTradeRow, batchSize),
	}, nil
}

func (r *ParquetTradeReader) Next() bool {
	if r.closed || r.err != nil {
		return false
	}

	if r.bufIdx >= r.bufLen {
		n, err := r.reader.Read(r.buffer)
		if n == 0 {
			if err != nil && err != io.EOF {
				r.err = err
			}
			return false
		}
		r.bufIdx = 0
		r.bufLen = n
	}

	row := r.buffer[r.bufIdx]
	r.bufIdx++

	sym := r.symbol
	if sym == "" {
		sym = row.Ticker
	}

	r.current = Tick{
		SIPTimestampNS: row.SIPTimestamp,
		Symbol:         sym,
		Type:           TickTrade,
		Price:          row.Price,
		Size:           row.Size,
	}
	return true
}

func (r *ParquetTradeReader) Tick() Tick {
	return r.current
}

func (r *ParquetTradeReader) Err() error {
	return r.err
}

func (r *ParquetTradeReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	var err1, err2 error
	if r.reader != nil {
		err1 = r.reader.Close()
	}
	if r.file != nil {
		err2 = r.file.Close()
	}
	if err1 != nil {
		return err1
	}
	return err2
}

// ParquetQuoteReader reads quote ticks from a Parquet file.
type ParquetQuoteReader struct {
	symbol  string
	file    *os.File
	reader  *parquet.GenericReader[parquetQuoteRow]
	buffer  []parquetQuoteRow
	bufIdx  int
	bufLen  int
	current Tick
	err     error
	closed  bool
}

func OpenQuoteReader(filePath string, symbol string, batchSize int) (*ParquetQuoteReader, error) {
	if batchSize <= 0 {
		batchSize = 2048
	}

	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open quotes parquet %s: %w", filePath, err)
	}

	reader := parquet.NewGenericReader[parquetQuoteRow](f)
	return &ParquetQuoteReader{
		symbol: symbol,
		file:   f,
		reader: reader,
		buffer: make([]parquetQuoteRow, batchSize),
	}, nil
}

func (r *ParquetQuoteReader) Next() bool {
	if r.closed || r.err != nil {
		return false
	}

	if r.bufIdx >= r.bufLen {
		n, err := r.reader.Read(r.buffer)
		if n == 0 {
			if err != nil && err != io.EOF {
				r.err = err
			}
			return false
		}
		r.bufIdx = 0
		r.bufLen = n
	}

	row := r.buffer[r.bufIdx]
	r.bufIdx++

	sym := r.symbol
	if sym == "" {
		sym = row.Ticker
	}

	mid := 0.0
	if row.BidPrice > 0 && row.AskPrice > 0 {
		mid = (row.BidPrice + row.AskPrice) / 2.0
	} else if row.BidPrice > 0 {
		mid = row.BidPrice
	} else {
		mid = row.AskPrice
	}

	r.current = Tick{
		SIPTimestampNS: row.SIPTimestamp,
		Symbol:         sym,
		Type:           TickQuote,
		Price:          mid,
		BidPx:          row.BidPrice,
		AskPx:          row.AskPrice,
		BidSz:          row.BidSize,
		AskSz:          row.AskSize,
	}
	return true
}

func (r *ParquetQuoteReader) Tick() Tick {
	return r.current
}

func (r *ParquetQuoteReader) Err() error {
	return r.err
}

func (r *ParquetQuoteReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	var err1, err2 error
	if r.reader != nil {
		err1 = r.reader.Close()
	}
	if r.file != nil {
		err2 = r.file.Close()
	}
	if err1 != nil {
		return err1
	}
	return err2
}
