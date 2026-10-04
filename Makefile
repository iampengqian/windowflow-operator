IMG ?= ghcr.io/iampengqian/windowflow-operator:v0.1.0
CONTROLLER_GEN_VERSION := v0.20.0

.PHONY: build test test-go test-python manifests fmt vet image install uninstall test-envtest

build:
	go build -trimpath -o bin/windowflow ./cmd/windowflow

test: test-go test-python

test-go:
	go test -race ./...

test-python:
	PYTHONPATH=sdk/python python3 -m unittest discover -s sdk/python/tests -v

manifests:
	go run sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION) object paths=./api/... crd output:crd:artifacts:config=config/crd/bases

fmt:
	gofmt -w api cmd internal

vet:
	go vet ./...

image:
	docker build -t $(IMG) .

install:
	kubectl apply -k config/default

uninstall:
	@echo 'Delete workload resources only after safe reader/transfer shutdown; see docs/operations.md.'

test-envtest:
	go test -tags=integration -v ./internal/controller
