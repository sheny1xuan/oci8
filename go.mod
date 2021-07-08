module github.com/sheny1xuan/oci8

go 1.10

require (
	github.com/go-playground/assert/v2 v2.0.1
	github.com/golang/protobuf v1.5.2
	github.com/google/go-cmp v0.5.5
	github.com/kr/text v0.2.0 // indirect
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/pingcap/log v0.0.0-20210625125904-98ed8e2eb1c7 // indirect
	github.com/pingcap/parser v0.0.0-20200424075042-8222d8b724a4
	github.com/pkg/errors v0.9.1
	github.com/transaction-wg/seata-golang v1.0.0-rc3
	go.uber.org/atomic v1.8.0 // indirect
	go.uber.org/multierr v1.7.0 // indirect
	go.uber.org/zap v1.18.1 // indirect
	golang.org/x/lint v0.0.0-20210508222113-6edffad5e616 // indirect
	golang.org/x/sys v0.0.0-20210630005230-0f9fa26af87c // indirect
	golang.org/x/text v0.3.6 // indirect
	golang.org/x/tools v0.1.4 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	vimagination.zapto.org/byteio v1.0.0
)

replace github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.4

replace go.etcd.io/bbolt => github.com/coreos/bbolt v1.3.4
