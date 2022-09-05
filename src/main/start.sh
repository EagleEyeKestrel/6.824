rm mr-*-*
go build -race -buildmode=plugin ../mrapps/wc.go
go run -race mrcoordinator.go pg-grimm.txt pg-tom_sawyer.txt