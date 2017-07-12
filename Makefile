SRC_FILES = $(wildcard *.go) $(wildcard consumer/*.go)

pg_warp: $(SRC_FILES)
	go build
	go test -v ./...
