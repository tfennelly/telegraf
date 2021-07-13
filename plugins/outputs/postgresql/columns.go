package postgresql

import "github.com/influxdata/telegraf/plugins/outputs/postgresql/utils"

// Column names and data types for standard fields (time, tag_id, tags, and fields)
const (
	TimeColumnName       = "time"
	TimeColumnDataType   = utils.PgTimestampWithTimeZone
	TagIDColumnName      = "tag_id"
	TagIDColumnDataType  = utils.PgBigInt
	TagsJSONColumnName   = "tags"
	FieldsJSONColumnName = "fields"
	JSONColumnDataType   = utils.PgJSONb
)

var TimeColumn = utils.Column{Name: TimeColumnName, Type: TimeColumnDataType, Role: utils.TimeColType}
var TagIDColumn = utils.Column{Name: TagIDColumnName, Type: TagIDColumnDataType, Role: utils.TagsIDColType}
var FieldsJSONColumn = utils.Column{Name: FieldsJSONColumnName, Type: JSONColumnDataType, Role: utils.FieldColType}
var TagsJSONColumn = utils.Column{Name: TagsJSONColumnName, Type: JSONColumnDataType, Role: utils.TagColType}

func ColumnFromTag(key string, value interface{}) utils.Column {
	return utils.Column{Name: key, Type: utils.DerivePgDatatype(value), Role: utils.TagColType}
}
func ColumnFromField(key string, value interface{}) utils.Column {
	return utils.Column{Name: key, Type: utils.DerivePgDatatype(value), Role: utils.FieldColType}
}
