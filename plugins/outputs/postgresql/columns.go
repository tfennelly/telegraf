package postgresql

import "github.com/influxdata/telegraf/plugins/outputs/postgresql/utils"

// Column names and data types for standard fields (time, tag_id, tags, and fields)
const (
	timeColumnName       = "time"
	timeColumnDataType   = utils.PgTimestampWithTimeZone
	tagIDColumnName      = "tag_id"
	tagIDColumnDataType  = utils.PgBigInt
	tagsJSONColumnName   = "tags"
	fieldsJSONColumnName = "fields"
	jsonColumnDataType   = utils.PgJSONb
)

var timeColumn = utils.Column{Name: timeColumnName, Type: timeColumnDataType, Role: utils.TimeColType}
var tagIDColumn = utils.Column{Name: tagIDColumnName, Type: tagIDColumnDataType, Role: utils.TagsIDColType}
var fieldsJSONColumn = utils.Column{Name: fieldsJSONColumnName, Type: jsonColumnDataType, Role: utils.FieldColType}
var tagsJSONColumn = utils.Column{Name: tagsJSONColumnName, Type: jsonColumnDataType, Role: utils.TagColType}

func columnFromTag(key string, value interface{}) utils.Column {
	return utils.Column{Name: key, Type: utils.DerivePgDatatype(value), Role: utils.TagColType}
}
func columnFromField(key string, value interface{}) utils.Column {
	return utils.Column{Name: key, Type: utils.DerivePgDatatype(value), Role: utils.FieldColType}
}
