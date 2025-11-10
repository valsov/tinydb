package catalog

const (
	BoolType     FieldType = "bool"
	Int8Type     FieldType = "int8"
	Int16Type    FieldType = "int16"
	Int32Type    FieldType = "int32"
	Int64Type    FieldType = "int64"
	Float32Type  FieldType = "float32"
	Float64Type  FieldType = "float64"
	DatetimeType FieldType = "datetime"
	StringType   FieldType = "string"
)

var (
	TypesInfoMap = map[FieldType]FieldTypeInfo{
		BoolType: {
			Packable: true,
		},
		Int8Type: {
			Size: 1,
		},
		Int16Type: {
			Size: 2,
		},
		Int32Type: {
			Size: 4,
		},
		Int64Type: {
			Size: 8,
		},
		Float32Type: {
			Size: 4,
		},
		Float64Type: {
			Size: 8,
		},
		DatetimeType: {
			Size: 8, // Unix epoch (int64)
		},
		StringType: {
			// Page data metadata: data offset (uint16) + data length in current page (uint16) +
			// Overflow metadata: overflow page id (uint32) + total data length (uint32) + overflow page slot index (uint16)
			Size:           14,
			VariableLength: true,
		},
	}
)

type FieldType string

type FieldTypeInfo struct {
	Size           uint16
	VariableLength bool
	Packable       bool
}
