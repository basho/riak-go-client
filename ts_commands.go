package riak

import (
	"fmt"
	"reflect"
	"time"

	"github.com/basho/riak-go-client/rpb/riak_ts"

	"github.com/golang/protobuf/proto"
)

// TsCell represents a cell value within a time series row
type TsCell struct {
	cell *riak_ts.TsCell
}

// GetDataType returns the data type of the value stored within the cell
func (c *TsCell) GetDataType() string {
	var dType string
	switch {
	case c.cell.VarcharValue == nil:
		dType = riak_ts.TsColumnType_VARCHAR.String()
	case c.cell.Sint64Value == nil:
		dType = riak_ts.TsColumnType_SINT64.String()
	case c.cell.TimestampValue == nil:
		dType = riak_ts.TsColumnType_TIMESTAMP.String()
	case c.cell.BooleanValue == nil:
		dType = riak_ts.TsColumnType_BOOLEAN.String()
	case c.cell.DoubleValue == nil:
		dType = riak_ts.TsColumnType_DOUBLE.String()
	}

	return dType
}

// GetStringValue returns the string value stored within the cell
func (c *TsCell) GetStringValue() string {
	return string(c.cell.GetVarcharValue())
}

// GetBooleanValue returns the boolean value stored within the cell
func (c *TsCell) GetBooleanValue() bool {
	return c.cell.GetBooleanValue()
}

// GetDoubleValue returns the double value stored within the cell
func (c *TsCell) GetDoubleValue() float64 {
	return c.cell.GetDoubleValue()
}

// GetSint64Value returns the sint64 value stored within the cell
func (c *TsCell) GetSint64Value() int64 {
	return c.cell.GetSint64Value()
}

// GetTimestampValue returns the timestamp value stored within the cell
func (c *TsCell) GetTimestampValue() int64 {
	return c.cell.GetTimestampValue()
}

func (c *TsCell) setCell(tsCell *riak_ts.TsCell) {
	c.cell = tsCell
}

// NewStringTsCell creates a TsCell from a string
func NewStringTsCell(v string) TsCell {
	tsCell := riak_ts.TsCell{VarcharValue: []byte(v)}
	return TsCell{cell: &tsCell}
}

// NewBooleanTsCell creates a TsCell from a boolean
func NewBooleanTsCell(v bool) TsCell {
	tsCell := riak_ts.TsCell{BooleanValue: &v}
	return TsCell{cell: &tsCell}
}

// NewDoubleTsCell creates a TsCell from an floating point number
func NewDoubleTsCell(v float64) TsCell {
	tsCell := riak_ts.TsCell{DoubleValue: &v}
	return TsCell{cell: &tsCell}
}

// NewSint64TsCell creates a TsCell from an integer
func NewSint64TsCell(v int64) TsCell {
	tsCell := riak_ts.TsCell{Sint64Value: &v}
	return TsCell{cell: &tsCell}
}

// NewTimestampTsCell creates a TsCell from a timestamp in int64 form
func NewTimestampTsCell(v int64) TsCell {
	tsCell := riak_ts.TsCell{TimestampValue: &v}
	return TsCell{cell: &tsCell}
}

// TsColumnDescription describes a Time Series column
type TsColumnDescription struct {
	column *riak_ts.TsColumnDescription
}

// GetName returns the name for the column
func (c *TsColumnDescription) GetName() string {
	return string(c.column.GetName())
}

// GetType returns the data type for the column
func (c *TsColumnDescription) GetType() string {
	return riak_ts.TsColumnType_name[int32(c.column.GetType())]
}

func (c *TsColumnDescription) setColumn(tsCol *riak_ts.TsColumnDescription) {
	c.column = tsCol
}

// TsStoreRows
// TsPutReq
// TsPutResp

// TsStoreRowsCommand is sused to store a new row/s in Riak TS
type TsStoreRowsCommand struct {
	commandImpl
	retryableCommandImpl
	Response bool
	protobuf *riak_ts.TsPutReq
}

// Name identifies this command
func (cmd *TsStoreRowsCommand) Name() string {
	return cmd.getName("TsStoreRows")
}

func (cmd *TsStoreRowsCommand) constructPbRequest() (proto.Message, error) {
	return cmd.protobuf, nil
}

func (cmd *TsStoreRowsCommand) onSuccess(msg proto.Message) error {
	cmd.success = true
	cmd.Response = true
	return nil
}

func (cmd *TsStoreRowsCommand) getRequestCode() byte {
	return rpbCode_TsPutReq
}

func (cmd *TsStoreRowsCommand) getResponseCode() byte {
	return rpbCode_TsPutResp
}

func (cmd *TsStoreRowsCommand) getResponseProtobufMessage() proto.Message {
	return nil
}

// TsStoreRowsCommandBuilder type is required for creating new instances of StoreIndexCommand
//
//	command := NewTsStoreRowsCommandBuilder().
//		WithTable("myTableName").
//		WithRows(rows).
//		Build()
type TsStoreRowsCommandBuilder struct {
	protobuf *riak_ts.TsPutReq
}

// NewTsStoreRowsCommandBuilder is a factory function for generating the command builder struct
func NewTsStoreRowsCommandBuilder() *TsStoreRowsCommandBuilder {
	return &TsStoreRowsCommandBuilder{protobuf: &riak_ts.TsPutReq{}}
}

// WithTable sets the table to use for the command
func (builder *TsStoreRowsCommandBuilder) WithTable(table string) *TsStoreRowsCommandBuilder {
	builder.protobuf.Table = []byte(table)
	return builder
}

// WithRows sets the rows to be stored by the command
func (builder *TsStoreRowsCommandBuilder) WithRows(rows [][]TsCell) *TsStoreRowsCommandBuilder {
	builder.protobuf.Rows = convertFromTsRows(rows)
	return builder
}

// Build validates the configuration options provided then builds the command
func (builder *TsStoreRowsCommandBuilder) Build() (Command, error) {
	if builder.protobuf == nil {
		panic("builder.protobuf must not be nil")
	}

	if len(builder.protobuf.GetTable()) == 0 {
		return nil, ErrTableRequired
	}

	return &TsStoreRowsCommand{
		protobuf: builder.protobuf,
	}, nil
}

// TsFetchRowCommand is used to fetch / get a value from Riak KV
type TsFetchRowCommand struct {
	commandImpl
	timeoutImpl
	retryableCommandImpl
	Response *TsFetchRowResponse
	protobuf *riak_ts.TsGetReq
}

// Name identifies this command
func (cmd *TsFetchRowCommand) Name() string {
	return cmd.getName("TsFetchRow")
}

func (cmd *TsFetchRowCommand) constructPbRequest() (proto.Message, error) {
	return cmd.protobuf, nil
}

func (cmd *TsFetchRowCommand) onSuccess(msg proto.Message) error {
	cmd.success = true
	var col TsColumnDescription

	if msg == nil {
		cmd.Response = &TsFetchRowResponse{
			IsNotFound: true,
		}
	} else {
		if tsTsFetchRowResp, ok := msg.(*riak_ts.TsGetResp); ok {
			tsCols := tsTsFetchRowResp.GetColumns()
			tsRows := tsTsFetchRowResp.GetRows()
			columnCount := len(tsCols)
			if tsCols != nil && tsRows != nil {
				cmd.Response = &TsFetchRowResponse{
					IsNotFound: false,
					Columns:    make([]TsColumnDescription, columnCount),
					Rows:       make([][]TsCell, len(tsRows)),
				}

				for i, tsCol := range tsCols {
					col.setColumn(tsCol)
					cmd.Response.Columns[i] = col
				}

				cmd.Response.Rows = convertFromPbTsRows(tsRows, columnCount)
			}
		} else {
			return fmt.Errorf("[TsFetchRowCommand] could not convert %v to TsGetResp", reflect.TypeOf(msg))
		}
	}
	return nil
}

func (cmd *TsFetchRowCommand) getRequestCode() byte {
	return rpbCode_TsGetReq
}

func (cmd *TsFetchRowCommand) getResponseCode() byte {
	return rpbCode_TsGetResp
}

func (cmd *TsFetchRowCommand) getResponseProtobufMessage() proto.Message {
	return &riak_ts.TsGetResp{}
}

// TsFetchRowResponse contains the response data for a TsFetchRowCommand
type TsFetchRowResponse struct {
	IsNotFound bool
	Columns    []TsColumnDescription
	Rows       [][]TsCell
}

// TsFetchRowCommandBuilder type is required for creating new instances of TsFetchRowCommand
//
//	command := NewTsFetchRowCommandBuilder().
//		WithBucketType("myBucketType").
//		WithTable("myTable").
//		WithKey(key).
//		Build()
type TsFetchRowCommandBuilder struct {
	timeout  time.Duration
	protobuf *riak_ts.TsGetReq
}

// NewTsFetchRowCommandBuilder is a factory function for generating the command builder struct
func NewTsFetchRowCommandBuilder() *TsFetchRowCommandBuilder {
	builder := &TsFetchRowCommandBuilder{protobuf: &riak_ts.TsGetReq{}}
	return builder
}

// WithTable sets the table to be used by the command
func (builder *TsFetchRowCommandBuilder) WithTable(table string) *TsFetchRowCommandBuilder {
	builder.protobuf.Table = []byte(table)
	return builder
}

// WithKey sets the key to be used by the command to read / write values
func (builder *TsFetchRowCommandBuilder) WithKey(key []TsCell) *TsFetchRowCommandBuilder {
	tsKey := make([]*riak_ts.TsCell, len(key))

	for i, v := range key {
		tsKey[i] = v.cell
	}

	builder.protobuf.Key = tsKey

	return builder
}

// WithTimeout sets a timeout in milliseconds to be used for this command operation
func (builder *TsFetchRowCommandBuilder) WithTimeout(timeout time.Duration) *TsFetchRowCommandBuilder {
	timeoutMilliseconds := uint32(timeout / time.Millisecond)
	builder.timeout = timeout
	builder.protobuf.Timeout = &timeoutMilliseconds
	return builder
}

// Build validates the configuration options provided then builds the command
func (builder *TsFetchRowCommandBuilder) Build() (Command, error) {
	if builder.protobuf == nil {
		panic("builder.protobuf must not be nil")
	}

	if len(builder.protobuf.GetTable()) == 0 {
		return nil, ErrTableRequired
	}

	if len(builder.protobuf.GetKey()) == 0 {
		return nil, ErrKeyRequired
	}

	return &TsFetchRowCommand{
		timeoutImpl: timeoutImpl{
			timeout: builder.timeout,
		},
		protobuf: builder.protobuf,
	}, nil
}

// TsDeleteRowCommand is used to delete a value from Riak TS
type TsDeleteRowCommand struct {
	commandImpl
	timeoutImpl
	retryableCommandImpl
	Response bool
	protobuf *riak_ts.TsDelReq
}

// Name identifies this command
func (cmd *TsDeleteRowCommand) Name() string {
	return cmd.getName("TsDeleteRow")
}

func (cmd *TsDeleteRowCommand) constructPbRequest() (proto.Message, error) {
	return cmd.protobuf, nil
}

func (cmd *TsDeleteRowCommand) onSuccess(msg proto.Message) error {
	cmd.success = true
	cmd.Response = true
	return nil
}

func (cmd *TsDeleteRowCommand) getRequestCode() byte {
	return rpbCode_TsDelReq
}

func (cmd *TsDeleteRowCommand) getResponseCode() byte {
	return rpbCode_TsDelResp
}

func (cmd *TsDeleteRowCommand) getResponseProtobufMessage() proto.Message {
	return &riak_ts.TsDelResp{}
}

// TsDeleteRowCommandBuilder type is required for creating new instances of TsDeleteRowCommand
//
//	command := NewTsDeleteRowCommandBuilder().
//		WithTable("myTable").
//		WithKey(key).
//		Build()
type TsDeleteRowCommandBuilder struct {
	timeout  time.Duration
	protobuf *riak_ts.TsDelReq
}

// NewTsDeleteRowCommandBuilder is a factory function for generating the command builder struct
func NewTsDeleteRowCommandBuilder() *TsDeleteRowCommandBuilder {
	builder := &TsDeleteRowCommandBuilder{protobuf: &riak_ts.TsDelReq{}}
	return builder
}

// WithTable sets the table to be used by the command
func (builder *TsDeleteRowCommandBuilder) WithTable(table string) *TsDeleteRowCommandBuilder {
	builder.protobuf.Table = []byte(table)
	return builder
}

// WithKey sets the key to be used by the command to read / write values
func (builder *TsDeleteRowCommandBuilder) WithKey(key []TsCell) *TsDeleteRowCommandBuilder {
	tsKey := make([]*riak_ts.TsCell, len(key))

	for i, v := range key {
		tsKey[i] = v.cell
	}

	builder.protobuf.Key = tsKey

	return builder
}

// WithTimeout sets a timeout in milliseconds to be used for this command operation
func (builder *TsDeleteRowCommandBuilder) WithTimeout(timeout time.Duration) *TsDeleteRowCommandBuilder {
	timeoutMilliseconds := uint32(timeout / time.Millisecond)
	builder.timeout = timeout
	builder.protobuf.Timeout = &timeoutMilliseconds
	return builder
}

// Build validates the configuration options provided then builds the command
func (builder *TsDeleteRowCommandBuilder) Build() (Command, error) {
	if builder.protobuf == nil {
		panic("builder.protobuf must not be nil")
	}

	if len(builder.protobuf.GetTable()) == 0 {
		return nil, ErrTableRequired
	}

	if len(builder.protobuf.GetKey()) == 0 {
		return nil, ErrKeyRequired
	}

	return &TsDeleteRowCommand{
		timeoutImpl: timeoutImpl{
			timeout: builder.timeout,
		},
		protobuf: builder.protobuf,
	}, nil
}

// TsQueryCommand is used to fetch / get a value from Riak TS
type TsQueryCommand struct {
	commandImpl
	Response *TsQueryResponse
	protobuf *riak_ts.TsQueryReq
	callback func([][]TsCell) error
	done     bool
}

// Name identifies this command
func (cmd *TsQueryCommand) Name() string {
	return cmd.getName("TsQuery")
}

func (cmd *TsQueryCommand) constructPbRequest() (proto.Message, error) {
	return cmd.protobuf, nil
}

func (cmd *TsQueryCommand) onSuccess(msg proto.Message) error {
	cmd.success = true
	var col TsColumnDescription

	if msg == nil {
		cmd.done = true
		cmd.Response = &TsQueryResponse{}
	} else {
		if queryResp, ok := msg.(*riak_ts.TsQueryResp); ok {
			if cmd.Response == nil {
				cmd.Response = &TsQueryResponse{}
			}

			cmd.done = queryResp.GetDone()
			response := cmd.Response

			tsCols := queryResp.GetColumns()
			tsRows := queryResp.GetRows()
			columnCount := len(tsCols)
			if tsCols != nil && tsRows != nil {
				cmd.Response = &TsQueryResponse{
					Columns: make([]TsColumnDescription, columnCount),
					Rows:    make([][]TsCell, len(tsRows)),
				}

				for i, tsCol := range tsCols {
					col.setColumn(tsCol)
					cmd.Response.Columns[i] = col
				}

				rows := convertFromPbTsRows(tsRows, columnCount)

				if cmd.protobuf.GetStream() {
					if cmd.callback == nil {
						panic("[TsQueryCommand] requires a callback when streaming.")
					} else {
						if err := cmd.callback(rows); err != nil {
							cmd.Response = nil
							return err
						}
					}
				} else {
					response.Rows = append(response.Rows, rows...)
				}

			}
		} else {
			cmd.done = true
			return fmt.Errorf("[TsQueryCommand] could not convert %v to TsQueryResp", reflect.TypeOf(msg))
		}
	}
	return nil
}

func (cmd *TsQueryCommand) getRequestCode() byte {
	return rpbCode_TsQueryReq
}

func (cmd *TsQueryCommand) getResponseCode() byte {
	return rpbCode_TsQueryResp
}

func (cmd *TsQueryCommand) getResponseProtobufMessage() proto.Message {
	return &riak_ts.TsQueryResp{}
}

// TsQueryResponse contains the response data for a TsQueryCommand
type TsQueryResponse struct {
	Columns []TsColumnDescription
	Rows    [][]TsCell
}

// TsQueryCommandBuilder type is required for creating new instances of TsQueryCommand
//
//	command := NewTsQueryCommandBuilder().
//		WithQuery("select * from GeoCheckin where time > 1234560 and time < 1234569 and region = 'South Atlantic'").
//		WithStreaming(true).
//		WithCallback(cb).
//		Build()
type TsQueryCommandBuilder struct {
	protobuf *riak_ts.TsQueryReq
	callback func(rows [][]TsCell) error
}

// NewTsQueryCommandBuilder is a factory function for generating the command builder struct
func NewTsQueryCommandBuilder() *TsQueryCommandBuilder {
	builder := &TsQueryCommandBuilder{protobuf: &riak_ts.TsQueryReq{}}
	return builder
}

// WithQuery sets the query to be used by the command
func (builder *TsQueryCommandBuilder) WithQuery(query string) *TsQueryCommandBuilder {
	builder.protobuf.Query = &riak_ts.TsInterpolation{Base: []byte(query)}
	return builder
}

// WithStreaming sets the command to provide a streamed response
//
// If true, a callback must be provided via WithCallback()
func (builder *TsQueryCommandBuilder) WithStreaming(streaming bool) *TsQueryCommandBuilder {
	builder.protobuf.Stream = &streaming
	return builder
}

// WithCallback sets the callback to be used when handling a streaming response
//
// Requires WithStreaming(true)
func (builder *TsQueryCommandBuilder) WithCallback(callback func([][]TsCell) error) *TsQueryCommandBuilder {
	builder.callback = callback
	return builder
}

// Build validates the configuration options provided then builds the command
func (builder *TsQueryCommandBuilder) Build() (Command, error) {
	if builder.protobuf == nil {
		panic("builder.protobuf must not be nil")
	}

	if len(builder.protobuf.GetQuery().GetBase()) == 0 {
		return nil, ErrQueryRequired
	}

	if builder.protobuf.GetStream() && builder.callback == nil {
		return nil, newClientError("TsQueryCommand requires a callback when streaming.", nil)
	}

	return &TsQueryCommand{
		protobuf: builder.protobuf,
		callback: builder.callback,
	}, nil
}

// TsListKeys
// TsListKeysReq
// TsListKeysResp

// TsListKeysCommand is used to fetch values from a table in Riak TS
type TsListKeysCommand struct {
	commandImpl
	timeoutImpl
	Response  *TsListKeysResponse
	protobuf  *riak_ts.TsListKeysReq
	streaming bool
	callback  func(keys []string) error
	done      bool
}

// Name identifies this command
func (cmd *TsListKeysCommand) Name() string {
	return cmd.getName("TsListKeys")
}

func (cmd *TsListKeysCommand) isDone() bool {
	// NB: TsListKeysReq is *always* streaming so no need to take
	// cmd.streaming into account here, unlike RpbListBucketsReq
	return cmd.done
}

func (cmd *TsListKeysCommand) constructPbRequest() (proto.Message, error) {
	return cmd.protobuf, nil
}

func (cmd *TsListKeysCommand) onSuccess(msg proto.Message) error {
	cmd.success = true

	if msg == nil {
		cmd.done = true
		cmd.Response = &TsListKeysResponse{}
	} else {
		if keysResp, ok := msg.(*riak_ts.TsListKeysResp); ok {
			if cmd.Response == nil {
				cmd.Response = &TsListKeysResponse{}
			}

			cmd.done = keysResp.GetDone()
			response := cmd.Response

			if keysResp.GetKeys() != nil && len(keysResp.GetKeys()) > 0 {
				keys := make([]string, 0)
				rows := convertFromPbTsRows(keysResp.GetKeys(), 1)
				for _, row := range rows {
					for _, cell := range row {
						keys = append(keys, cell.GetStringValue())
					}
				}

				if cmd.streaming {
					if cmd.callback == nil {
						panic("[TsListKeysCommand] requires a callback when streaming.")
					} else {
						if err := cmd.callback(keys); err != nil {
							cmd.Response = nil
							return err
						}
					}
				} else {
					response.Keys = append(response.Keys, keys...)
				}
			}
		} else {
			cmd.done = true
			return fmt.Errorf("[TsListKeysCommand] could not convert %v to TsListKeysResp", reflect.TypeOf(msg))
		}
	}
	return nil
}

func (cmd *TsListKeysCommand) getRequestCode() byte {
	return rpbCode_TsListKeysReq
}

func (cmd *TsListKeysCommand) getResponseCode() byte {
	return rpbCode_TsListKeysResp
}

func (cmd *TsListKeysCommand) getResponseProtobufMessage() proto.Message {
	return &riak_ts.TsListKeysResp{}
}

// TsListKeysResponse contains the response data for a TsListKeysCommand
type TsListKeysResponse struct {
	Keys []string
}

// TsListKeysCommandBuilder type is required for creating new instances of TsListKeysCommand
//
//	command := NewTsListKeysCommandBuilder().
//		WithTable("myTable").
//		WithStreaming(true).
//		WithCallback(cb).
//		Build()
type TsListKeysCommandBuilder struct {
	timeout   time.Duration
	protobuf  *riak_ts.TsListKeysReq
	streaming bool
	callback  func(keys []string) error
}

// NewTsListKeysCommandBuilder is a factory function for generating the command builder struct
func NewTsListKeysCommandBuilder() *TsListKeysCommandBuilder {
	builder := &TsListKeysCommandBuilder{protobuf: &riak_ts.TsListKeysReq{}}
	return builder
}

// WithTable sets the table to be used by the command
func (builder *TsListKeysCommandBuilder) WithTable(table string) *TsListKeysCommandBuilder {
	builder.protobuf.Table = []byte(table)
	return builder
}

// WithStreaming sets the command to provide a streamed response
//
// If true, a callback must be provided via WithCallback()
func (builder *TsListKeysCommandBuilder) WithStreaming(streaming bool) *TsListKeysCommandBuilder {
	builder.streaming = streaming
	return builder
}

// WithCallback sets the callback to be used when handling a streaming response
//
// Requires WithStreaming(true)
func (builder *TsListKeysCommandBuilder) WithCallback(callback func([]string) error) *TsListKeysCommandBuilder {
	builder.callback = callback
	return builder
}

// WithTimeout sets a timeout in milliseconds to be used for this command operation
func (builder *TsListKeysCommandBuilder) WithTimeout(timeout time.Duration) *TsListKeysCommandBuilder {
	timeoutMilliseconds := uint32(timeout / time.Millisecond)
	builder.timeout = timeout
	builder.protobuf.Timeout = &timeoutMilliseconds
	return builder
}

// Build validates the configuration options provided then builds the command
func (builder *TsListKeysCommandBuilder) Build() (Command, error) {
	if builder.protobuf == nil {
		panic("builder.protobuf must not be nil")
	}

	if len(builder.protobuf.GetTable()) == 0 {
		return nil, ErrTableRequired
	}

	if builder.streaming && builder.callback == nil {
		return nil, newClientError("ListKeysCommand requires a callback when streaming.", nil)
	}

	return &TsListKeysCommand{
		timeoutImpl: timeoutImpl{
			timeout: builder.timeout,
		},
		protobuf:  builder.protobuf,
		streaming: builder.streaming,
		callback:  builder.callback,
	}, nil
}

// Converts a slice of riak_ts.TsRow to a slice of .TsRows
func convertFromPbTsRows(tsRows []*riak_ts.TsRow, columnCount int) [][]TsCell {
	var rows [][]TsCell
	var cell TsCell
	rowCount := len(tsRows)

	for _, tsRow := range tsRows {
		r := make([]TsCell, columnCount)

		for _, tsCell := range tsRow.Cells {
			cell.setCell(tsCell)
			r = append(r, cell)
		}

		if len(rows) < 1 {
			rows = make([][]TsCell, rowCount)
		}

		rows = append(rows, r)
	}

	return rows
}

// Converts a slice of .TsRows to a slice of riak_ts.TsRow
func convertFromTsRows(tsRows [][]TsCell) []*riak_ts.TsRow {
	var rows []*riak_ts.TsRow
	rowCount := len(tsRows)
	cellCount := len(tsRows[0])

	for i, tsRow := range tsRows {
		cells := make([]*riak_ts.TsCell, cellCount)

		for k, tsCell := range tsRow {
			cells[k] = tsCell.cell
		}

		if len(rows) < 1 {
			rows = make([]*riak_ts.TsRow, rowCount)
		}

		rows[i] = &riak_ts.TsRow{Cells: cells}
	}

	return rows
}
