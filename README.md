# fpm-go-plugin-mongo

### Install

`$ go get -u github.com/team4yf/fpm-go-plugin-mongo`

```golang

import _ "github.com/team4yf/fpm-go-plugin-mongo/plugin"

```

### Config

`conf/config.local.yaml`

```yaml
mongo:
    foo: bar
```

### Usage

```golang

fpmApp.Execute("mongo.demo", &fpm.BizParam{
    "body":    "ok",
})

```