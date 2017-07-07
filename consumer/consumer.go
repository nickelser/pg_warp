package consumer

import (
	"fmt"
	"strings"
)

const (
	parseStateNull              = iota
	parseStateEscapedIdentifier = iota
	parseStateEscapedLiteral    = iota
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
			} else if chr == '(' && message[state.tokenStart:i] == "(no-tuple-data)" {
				result.noTupleData = true
				state.current = parseStateEnd
			} else if chr == '"' {
				state.prev = state.current
				state.current = parseStateEscapedIdentifier
			}
		case parseStateColumnDataType:
			if chr == ']' {
				if chrNext != ':' {
					return result, fmt.Errorf("Invalid character ':' at %d", i+1)
				}
				state.currentColumn.dataType = message[state.tokenStart:i]
				state.tokenStart = i + 2
				state.current = parseStateColumnValue
			} else if chr == '"' {
				state.prev = state.current
				state.current = parseStateEscapedIdentifier
			}
		case parseStateColumnValue:
			if chr == '\000' {
				state.currentColumn.value = message[state.tokenStart:i]
				result.columns = append(result.columns, state.currentColumn)
				state.current = parseStateEnd
			} else if chr == ' ' {
				state.currentColumn.value = message[state.tokenStart:i]
				if state.oldKey {
					result.oldKey = append(result.columns, state.currentColumn)
				} else {
					result.columns = append(result.columns, state.currentColumn)
				}
				state.tokenStart = i + 1
				state.current = parseStateColumnName
			} else if chr == '\'' {
				state.prev = state.current
				state.current = parseStateEscapedLiteral
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

func Decode(message string, replicaIdentities map[string][]string) (string, string) {
	if strings.HasPrefix(message, "BEGIN") {
		return "BEGIN", "BEGIN"
	}
	if strings.HasPrefix(message, "COMMIT") {
		return "COMMIT", "COMMIT"
	}

	if strings.HasPrefix(message, "table ") {
		parseResult, err := parseTableMessage(message)
		if err != nil {
			fmt.Printf("Parser error: %+v\n", err)
			return "", ""
		}
		if parseResult.noTupleData {
			fmt.Printf("No tuple data: %s\n", message)
			return "", ""
		}

		targetRelation := parseResult.relation
		replicaIdentity := replicaIdentities[targetRelation]
		action := parseResult.action

		columnNames := []string{}
		columnValues := []string{}
		for _, column := range parseResult.columns {
			if column.value == "unchanged-toast-datum" {
				continue
			}
			columnNames = append(columnNames, column.name)
			columnValues = append(columnValues, column.value)
		}

		switch action {
		case "INSERT":
			return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", targetRelation, strings.Join(columnNames, ", "), strings.Join(columnValues, ", ")), "INSERT"
		case "UPDATE":
			whereClause := ""
			if len(parseResult.oldKey) > 0 {
				for _, column := range parseResult.oldKey {
					if whereClause != "" {
						whereClause += " AND "
					}
					whereClause += column.name + " = " + column.value
				}
			} else {
				// TODO: Failed to handle WAL message: Could not apply stream to target: ERROR: modifying the partition value of rows is not allowed (SQLSTATE 0A000)
				// We should not include the partition value again
				for _, column := range parseResult.columns {
					identityColumn := false
					for _, identityColumnName := range replicaIdentity {
						if column.name == identityColumnName {
							identityColumn = true
						}
					}
					if !identityColumn {
						continue
					}
					if whereClause != "" {
						whereClause += " AND "
					}
					whereClause += column.name + " = " + column.value
				}
				if whereClause == "" {
					fmt.Printf("No known replica identity: %s\n", message)
					return "", ""
				}
			}
			return fmt.Sprintf("UPDATE %s SET (%s) = (%s) WHERE %s", targetRelation, strings.Join(columnNames, ", "), strings.Join(columnValues, ", "), whereClause), "UPDATE"
		case "DELETE":
			// The column/data values here are the replica identity (i.e. what we want to match on)
			whereClause := ""
			for _, column := range parseResult.columns {
				if whereClause != "" {
					whereClause += " AND "
				}
				whereClause += column.name + " = " + column.value
			}
			return fmt.Sprintf("DELETE FROM %s WHERE %s", targetRelation, whereClause), "DELETE"
		}
	}
	return "", ""
}
