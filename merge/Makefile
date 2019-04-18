all: clean merge

Project = github.com/dearcode/doodle/service/debug.Project
GitHash = github.com/dearcode/doodle/service/debug.GitHash
GitTime = github.com/dearcode/doodle/service/debug.GitTime
GitMessage = github.com/dearcode/doodle/service/debug.GitMessage

LDFLAGS += -X "$(Project)=github.com/binlake/merge"
LDFLAGS += -X "$(GitHash)=$(shell git log --pretty=format:'%H' -1)"
LDFLAGS += -X "$(GitTime)=$(shell git log --pretty=format:'%ct' -1)"
LDFLAGS += -X "$(GitMessage)=$(shell git log --pretty=format:'%cn %s %b' -1)"

FILES := $$(find . -name '*.go' | grep -vE 'vendor')
SOURCE_PATH := binlog cfs handler mode regx

golint:
	go get golang.org/x/lint/golint

megacheck:
	go get honnef.co/go/tools/cmd/megacheck

lint: golint megacheck
	@for path in $(SOURCE_PATH); do echo "golint $$path"; golint $$path"/..."; done;
	@for path in $(SOURCE_PATH); do echo "gofmt -s -l -w $$path";  gofmt -s -l -w $$path;  done;
	@for f in $(FILES); do go vet $$f 2>&1; done;
	megacheck ./...

clean:
	@rm -rf merge 

merge:
	go build -o $@ -ldflags '$(LDFLAGS)' 

