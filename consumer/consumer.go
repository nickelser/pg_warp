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

	currentColumn ParseResultColumn
	oldKey        bool
}

// ParseResultColumn - Column data from the decoding message
type ParseResultColumn struct {
	Name     string
	DataType string
	Value    string
}

// ParseResult - Data contained in the decoding message as a struct
type ParseResult struct {
	Relation string
	Action   string
	Columns  []ParseResultColumn

	NoTupleData bool
	OldKey      []ParseResultColumn
}

func parseTableMessage(message string) (ParseResult, error) {
	state := parseState{tokenStart: 6, current: parseStateRelation}
	result := ParseResult{}

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
				result.Relation = message[state.tokenStart:i]
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
				result.Action = message[state.tokenStart:i]
				state.tokenStart = i + 2
				state.current = parseStateColumnName
			}
		case parseStateColumnName:
			if chr == '[' {
				state.currentColumn = ParseResultColumn{Name: message[state.tokenStart:i]}
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
				result.NoTupleData = true
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
				state.currentColumn.DataType = message[state.tokenStart:i]
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
				state.currentColumn.Value = message[state.tokenStart:i]
				result.Columns = append(result.Columns, state.currentColumn)
				state.current = parseStateEnd
			} else if chr == ' ' {
				state.currentColumn.Value = message[state.tokenStart:i]
				if state.oldKey {
					result.OldKey = append(result.OldKey, state.currentColumn)
				} else {
					result.Columns = append(result.Columns, state.currentColumn)
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

// Decode - Decodes a logical decoding message into SQL
func Decode(message string, replicaIdentities map[string][]string, distributedTables map[string]string) (string, ParseResult) {
	if strings.HasPrefix(message, "BEGIN") {
		return "BEGIN", ParseResult{Action: "BEGIN"}
	}
	if strings.HasPrefix(message, "COMMIT") {
		return "COMMIT", ParseResult{Action: "COMMIT"}
	}

	if strings.HasPrefix(message, "table ") {
		parseResult, err := parseTableMessage(message)
		if err != nil {
			fmt.Printf("ERROR: Parser error: %+v\n", err)
			fmt.Printf("HINT: Original message: %s\n", message)
			os.Exit(1)
		}

		targetRelation := parseResult.Relation
		replicaIdentity, ok := replicaIdentities[targetRelation]
		if !ok {
			return "", parseResult
		}

		if parseResult.NoTupleData {
			fmt.Printf("WARN: Missing primary key for table %s, ignoring %s\n", targetRelation, parseResult.Action)
			fmt.Printf("HINT: Original message: %s\n", message)
			return "", parseResult
		}

		switch parseResult.Action {
		case "INSERT":
			columnNames := []string{}
			columnValues := []string{}
			for _, column := range parseResult.Columns {
				columnNames = append(columnNames, column.Name)
				columnValues = append(columnValues, column.Value)
			}
			return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", targetRelation, strings.Join(columnNames, ", "), strings.Join(columnValues, ", ")), parseResult
		case "UPDATE":
			partitionColumn, isDistributed := distributedTables[targetRelation]
			setClauses := []string{}
			whereClauses := []string{}
			if len(parseResult.OldKey) != 0 {
				partitionColumnIncluded := false
				for _, column := range parseResult.OldKey {
					whereClauses = append(whereClauses, column.Name+" = "+column.Value)
					if isDistributed && column.Name == partitionColumn {
						partitionColumnIncluded = true
					}
				}
				if isDistributed && !partitionColumnIncluded {
					for _, column := range parseResult.Columns {
						if column.Name == partitionColumn {
							whereClauses = append(whereClauses, column.Name+" = "+column.Value)
						}
					}
				}
			} else {
				for _, column := range parseResult.Columns {
					identityColumn := false
					for _, identityColumnName := range replicaIdentity {
						if column.Name == identityColumnName {
							identityColumn = true
						}
					}
					if isDistributed && column.Name == partitionColumn {
						identityColumn = true
					}
					if !identityColumn {
						continue
					}
					whereClauses = append(whereClauses, column.Name+" = "+column.Value)
				}
				if len(whereClauses) == 0 {
					fmt.Printf("WARN: Missing primary key for table %s, ignoring %s\n", targetRelation, parseResult.Action)
					fmt.Printf("HINT: Original message: %s\n", message)
					return "", parseResult
				}
			}

			for _, column := range parseResult.Columns {
				if column.Value == "unchanged-toast-datum" {
					continue
				}
				if isDistributed && column.Name == partitionColumn {
					continue
				}
				if len(parseResult.OldKey) == 0 {
					identityColumn := false
					for _, identityColumnName := range replicaIdentity {
						if column.Name == identityColumnName {
							identityColumn = true
						}
					}
					if identityColumn {
						continue
					}
				}
				setClauses = append(setClauses, column.Name+" = "+column.Value)
			}
			return fmt.Sprintf("UPDATE %s SET %s WHERE %s", targetRelation, strings.Join(setClauses, ", "), strings.Join(whereClauses, " AND ")), parseResult
		case "DELETE":
			// The column/data values here are the replica identity (i.e. what we want to match on)
			whereClauses := []string{}
			for _, column := range parseResult.Columns {
				whereClauses = append(whereClauses, column.Name+" = "+column.Value)
			}
			return fmt.Sprintf("DELETE FROM %s WHERE %s", targetRelation, strings.Join(whereClauses, " AND ")), parseResult
		default:
			return "", parseResult
		}
	}
	return "", ParseResult{}
}
