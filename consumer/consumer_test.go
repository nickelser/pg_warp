package consumer_test

import (
	"testing"

	"github.com/citusdata/pg_warp/consumer"
)

var decodertests = []struct {
	in  string
	out string
}{
	{"BEGIN", "BEGIN"},
	{"COMMIT", "COMMIT"},
	{"table public.data: INSERT: id[int4]:2 data[text]:'arg'", "INSERT INTO public.data (id, data) VALUES (2, 'arg')"},
	{"table public.data: INSERT: id[int4]:3 whatever[text]:'new ''value' data[text]:'demo' moredata[jsonb]:null", "INSERT INTO public.data (id, whatever, data, moredata) VALUES (3, 'new ''value', 'demo', null)"},
	{"table public.z: UPDATE: id[integer]:1 another[text]:'value'", "UPDATE public.z SET another = 'value' WHERE id = 1"},
	{"table public.z: UPDATE: old-key: id[integer]:-1000 new-tuple: id[integer]:-2000 whatever[text]:'depesz'", "UPDATE public.z SET id = -2000, whatever = 'depesz' WHERE id = -1000"},
	{"table public.xyz: UPDATE: id[integer]:1 large[text]:unchanged-toast-datum small[text]:'value'", "UPDATE public.xyz SET small = 'value' WHERE id = 1"},
	{"table public.z: UPDATE: id[integer]:4 a[character varying]:'123' b[timestamp without time zone]:'2017-01-01 00:00:00' c[json]:'{\"a\":\"b\"}' d[citext[]]:'{}' e[text[][]]:'{}'", "UPDATE public.z SET a = '123', b = '2017-01-01 00:00:00', c = '{\"a\":\"b\"}', d = '{}', e = '{}' WHERE id = 4"},
	{"table public.data: DELETE: id[int4]:2", "DELETE FROM public.data WHERE id = 2"},
	{"table public.xyz: DELETE: id[int4]:3", "DELETE FROM public.xyz WHERE id = 3"},
	{"table public.data: DELETE: (no-tuple-data)", ""},
	{"table public.data: INSERT: \"'\"[integer]:1 \"\"\"\"[integer]:2 \" abc abc \"[integer]:3 another_column[\" why ' would \"\" you do this\"]:'sad'", "INSERT INTO public.data (\"'\", \"\"\"\", \" abc abc \", another_column) VALUES (1, 2, 3, 'sad')"},
	{"table public.\"test spaces\": INSERT: id[integer]:'\n'", "INSERT INTO public.\"test spaces\" (id) VALUES ('\n')"},
	{"table public.\"test ' \"\" quotes\": UPDATE: old-key: test[text]:'\\ab' new-tuple: test[text]:''' \" '", "UPDATE public.\"test ' \"\" quotes\" SET test = ''' \" ' WHERE test = '\\ab'"},
	{"table public.citus_test: DELETE: id[int4]:3", "SELECT master_modify_multiple_shards('DELETE FROM public.citus_test WHERE id = 3')"},
	{"table public.citus_test: UPDATE: id[int4]:1 customer_id[int4]:12 data[text]:'value'", "UPDATE public.citus_test SET data = 'value' WHERE id = 1 AND customer_id = 12"},
	{"table public.citus_test: UPDATE: old-key: id[integer]:-1000 new-tuple: id[integer]:-2000 customer_id[int4]:12 data[text]:'value'", "UPDATE public.citus_test SET id = -2000, data = 'value' WHERE id = -1000 AND customer_id = 12"},
	{"table public.citus_test2: UPDATE: old-key: id[integer]:-1000 customer_id[int4]:12 new-tuple: id[integer]:-2000 customer_id[int4]:12 data[text]:'value'", "UPDATE public.citus_test2 SET id = -2000, data = 'value' WHERE id = -1000 AND customer_id = 12"},
}

var replicaIdentities = map[string][]string{
	"public.z":                      {"id"},
	"public.xyz":                    {"id"},
	"public.data":                   {},
	"public.\"test spaces\"":        {},
	"public.\"test ' \"\" quotes\"": {},
	"public.citus_test":             {"id"},
	"public.citus_test2":            {"id", "customer_id"},
}

var distributedTables = map[string]string{
	"public.xyz":         "id",
	"public.citus_test":  "customer_id",
	"public.citus_test2": "customer_id",
}

func TestDecoder(t *testing.T) {
	for _, tt := range decodertests {
		s, _ := consumer.Decode(tt.in, replicaIdentities, distributedTables)
		if s != tt.out {
			t.Errorf("decode(%q)\n got: %q\n want: %q", tt.in, s, tt.out)
		}
	}
}
