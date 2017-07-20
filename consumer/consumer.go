package consumer

import (
	"fmt"
	"os"
	"strings"
)

const (
	parseStateNull              = iota
	parseStateEscapedIdentifier = iota
	parseStateEscapedLiteral    = iota
	parseStateOpenSquareBracket = iota
	parseStateEnd               = iota

	parseStateRelation       = iota
	parseStateAction         = iota
	parseStateColumnDataType = iota
	parseStateColumnName     = iota
	parseStateColumnValue    = iota
)

type parseState struct {
	current    int
	prev       int
	tokenStart int

	currentColumn parseResultColumn
	oldKey        bool
}

type parseResultColumn struct {
	name     string
	dataType string
	value    string
}

type parseResult struct {
	relation string
	action   string
	columns  []parseResultColumn

	noTupleData bool
	oldKey      []parseResultColumn
}

func parseTableMessage(message string) (parseResult, error) {
	state := parseState{tokenStart: 6, current: parseStateRelation}
	result := parseResult{}

	for i := 0; i <= len(message); i++ {
		if state.tokenStart > i {
			continue
		}

		chr := byte('\000')
		if len(message) > i {
			chr = message[i]
		}
		chrNext := byte('\000')
		if len(message) > i+1 {
			chrNext = message[i+1]
		}

		switch state.current {
		case parseStateNull:
			return result, fmt.Errorf("Invalid parse state null: %+v", state)
		case parseStateRelation:
			if chr == ':' {
				if chrNext != ' ' {
					return result, fmt.Errorf("Invalid character ' ' at %d", i+1)
				}
				result.relation = message[state.tokenStart:i]
				state.tokenStart = i + 2
				state.current = parseStateAction
			} else if chr == '"' {
				state.prev = state.current
				state.current = parseStateEscapedIdentifier
			}
		case parseStateAction:
			if chr == ':' {
				if chrNext != ' ' {
					return result, fmt.Errorf("Invalid character ' ' at %d", i+1)
				}
				result.action = message[state.tokenStart:i]
				state.tokenStart = i + 2
				state.current = parseStateColumnName
			}
		case parseStateColumnName:
			if chr == '[' {
				state.currentColumn = parseResultColumn{name: message[state.tokenStart:i]}
				state.tokenStart = i + 1
				state.current = parseStateColumnDataType
			} else if chr == ':' {
				if message[state.tokenStart:i] == "old-key" {
					state.oldKey = true
				} else if message[state.tokenStart:i] == "new-tuple" {
					state.oldKey = false
				}
				state.tokenStart = i + 2
			} else if chr == '(' && message[state.tokenStart:len(message)] == "(no-tuple-data)" {
				result.noTupleData = true
				state.current = parseStateEnd
			} else if chr == '"' {
				state.prev = state.current
				state.current = parseStateEscapedIdentifier
			}
		case parseStateColumnDataType:
			if chr == ']' {
				if chrNext != ':' {
					return result, fmt.Errorf("Invalid character '%s' at %d", []byte{chrNext}, i+1)
				}
				state.currentColumn.dataType = message[state.tokenStart:i]
				state.tokenStart = i + 2
				state.current = parseStateColumnValue
			} else if chr == '"' {
				state.prev = state.current
				state.current = parseStateEscapedIdentifier
			} else if chr == '[' {
				state.prev = state.current
				state.current = parseStateOpenSquareBracket
			}
		case parseStateColumnValue:
			if chr == '\000' {
				state.currentColumn.value = message[state.tokenStart:i]
				result.columns = append(result.columns, state.currentColumn)
				state.current = parseStateEnd
			} else if chr == ' ' {
				state.currentColumn.value = message[state.tokenStart:i]
				if state.oldKey {
					result.oldKey = append(result.oldKey, state.currentColumn)
				} else {
					result.columns = append(result.columns, state.currentColumn)
				}
				state.tokenStart = i + 1
				state.current = parseStateColumnName
			} else if chr == '\'' {
				state.prev = state.current
				state.current = parseStateEscapedLiteral
			}
		case parseStateOpenSquareBracket:
			if chr == ']' {
				state.current = state.prev
				state.prev = parseStateNull
			}
		case parseStateEscapedIdentifier:
			if chr == '"' {
				if chrNext == '"' {
					i++
				} else {
					state.current = state.prev
					state.prev = parseStateNull
				}
			}
		case parseStateEscapedLiteral:
			if chr == '\'' {
				if chrNext == '\'' {
					i++
				} else {
					state.current = state.prev
					state.prev = parseStateNull
				}
			}
		}
	}

	if state.current != parseStateEnd {
		return result, fmt.Errorf("Invalid parser end state: %+v", state.current)
	}

	return result, nil
}

func Decode(message string, replicaIdentities map[string][]string, distributedTables map[string]string) (string, string) {
	if strings.HasPrefix(message, "BEGIN") {
		return "BEGIN", "BEGIN"
	}
	if strings.HasPrefix(message, "COMMIT") {
		return "COMMIT", "COMMIT"
	}

	if strings.HasPrefix(message, "table ") {
		parseResult, err := parseTableMessage(message)
		if err != nil {
			fmt.Printf("ERROR: Parser error: %+v\n", err)
			fmt.Printf("HINT: Original message: %s\n", message)
			os.Exit(1)
		}

		targetRelation := parseResult.relation
		replicaIdentity, ok := replicaIdentities[targetRelation]
		if !ok {
			return "", ""
		}

		if parseResult.noTupleData {
			fmt.Printf("WARN: Missing primary key for table %s, ignoring %s\n", targetRelation, parseResult.action)
			fmt.Printf("HINT: Original message: %s\n", message)
			return "", ""
		}

		switch parseResult.action {
		case "INSERT":
			columnNames := []string{}
			columnValues := []string{}
			for _, column := range parseResult.columns {
				columnNames = append(columnNames, column.name)
				columnValues = append(columnValues, column.value)
			}
			return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", targetRelation, strings.Join(columnNames, ", "), strings.Join(columnValues, ", ")), "INSERT"
		case "UPDATE":
			partitionColumn, isDistributed := distributedTables[targetRelation]
			setClauses := []string{}
			whereClauses := []string{}
			if len(parseResult.oldKey) != 0 {
				partitionColumnIncluded := false
				for _, column := range parseResult.oldKey {
					whereClauses = append(whereClauses, column.name+" = "+column.value)
					if isDistributed && column.name == partitionColumn {
						partitionColumnIncluded = true
					}
				}
				if isDistributed && !partitionColumnIncluded {
					for _, column := range parseResult.columns {
						if column.name == partitionColumn {
							whereClauses = append(whereClauses, column.name+" = "+column.value)
						}
					}
				}
			} else {
				for _, column := range parseResult.columns {
					identityColumn := false
					for _, identityColumnName := range replicaIdentity {
						if column.name == identityColumnName {
							identityColumn = true
						}
					}
					if isDistributed && column.name == partitionColumn {
						identityColumn = true
					}
					if !identityColumn {
						continue
					}
					whereClauses = append(whereClauses, column.name+" = "+column.value)
				}
				if len(whereClauses) == 0 {
					fmt.Printf("WARN: Missing primary key for table %s, ignoring %s\n", targetRelation, parseResult.action)
					fmt.Printf("HINT: Original message: %s\n", message)
					return "", ""
				}
			}

			for _, column := range parseResult.columns {
				if column.value == "unchanged-toast-datum" {
					continue
				}
				if isDistributed && column.name == partitionColumn {
					continue
				}
				if len(parseResult.oldKey) == 0 {
					identityColumn := false
					for _, identityColumnName := range replicaIdentity {
						if column.name == identityColumnName {
							identityColumn = true
						}
					}
					if identityColumn {
						continue
					}
				}
				setClauses = append(setClauses, column.name+" = "+column.value)
			}
			return fmt.Sprintf("UPDATE %s SET %s WHERE %s", targetRelation, strings.Join(setClauses, ", "), strings.Join(whereClauses, " AND ")), "UPDATE"
		case "DELETE":
			// The column/data values here are the replica identity (i.e. what we want to match on)
			whereClauses := []string{}
			for _, column := range parseResult.columns {
				whereClauses = append(whereClauses, column.name+" = "+column.value)
			}
			sql := fmt.Sprintf("DELETE FROM %s WHERE %s", targetRelation, strings.Join(whereClauses, " AND "))

			// Handle special case where the destination has the partition column in the
			// primary key, but the source does not
			partitionColumn, isDistributed := distributedTables[targetRelation]
			if isDistributed {
				modifiesMultipleShards := true
				for _, column := range parseResult.columns {
					if column.name == partitionColumn {
						modifiesMultipleShards = false
					}
				}
				if modifiesMultipleShards {
					sql = fmt.Sprintf("SELECT master_modify_multiple_shards('%s')", strings.Replace(sql, "'", "''", -1))
				}
			}
			return sql, "DELETE"
		}
	}
	return "", ""
}
