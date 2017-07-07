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
	{"table public.z: UPDATE: id[integer]:1 another[text]:'value'", "UPDATE public.z SET (id, another) = (1, 'value') WHERE id = 1"},
	{"table public.z: UPDATE: old-key: id[integer]:-1000 new-tuple: id[integer]:-2000 whatever[text]:'depesz'", "UPDATE public.z SET (id, whatever) = (-2000, 'depesz') WHERE id = -1000"},
	{"table public.xyz: UPDATE: id[integer]:1 large[text]:unchanged-toast-datum small[text]:'value'", "UPDATE public.xyz SET (id, small) = (1, 'value') WHERE id = 1"},
	{"table public.data: DELETE: id[int4]:2", "DELETE FROM public.data WHERE id = 2"},
	{"table public.data: DELETE: id[int4]:3", "DELETE FROM public.data WHERE id = 3"},
}

var replicaIdentities = map[string][]string{
	"public.z":   {"id"},
	"public.xyz": {"id"},
}

func TestDecoder(t *testing.T) {
	for _, tt := range decodertests {
		s, _ := consumer.Decode(tt.in, replicaIdentities)
		if s != tt.out {
			t.Errorf("decode(%q)\n got: %q\n want: %q", tt.in, s, tt.out)
		}
	}
}
