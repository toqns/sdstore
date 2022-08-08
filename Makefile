VERSION := 0.2.4

# ==============================================================================
# Modules support

deps-reset:
	git checkout -- go.mod
	go mod tidy
	go mod vendor

tidy:
	go mod tidy
	go mod vendor

deps-upgrade:
	# go get $(go list -f '{{if not (or .Main .Indirect)}}{{.Path}}{{end}}' -m all)
	go get -u -v ./...
	go mod tidy
	go mod vendor

deps-cleancache:
	go clean -modcache

list:
	go list -mod=mod all

tagged-push:
	git tag v$(VERSION)
	git push origin v$(VERSION)

# ==============================================================================
# Running tests on the local computer

test:
	go test ./... -race -count=1
	staticcheck -checks=all ./...

test-verbose:
	go test ./... -v -race -count=1
	staticcheck -checks=all ./...