package plugin

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/team4yf/yf-fpm-server-go/fpm"
)

func TestFind(t *testing.T) {
	fpmApp := fpm.New()

	fpmApp.Init()

	data, err := fpmApp.Execute("mongo.find", &fpm.BizParam{
		"collection": "customer",
	})

	assert.Nil(t, err, "should not err")

	assert.NotNil(t, data, "should not nil")
}

func TestFirst(t *testing.T) {
	fpmApp := fpm.New()

	fpmApp.Init()

	data, err := fpmApp.Execute("mongo.first", &fpm.BizParam{
		"collection": "customer",
	})

	assert.Nil(t, err, "should not err")

	assert.NotNil(t, data, "should not nil")
}

func TestCreate(t *testing.T) {
	fpmApp := fpm.New()

	fpmApp.Init()

	data, err := fpmApp.Execute("mongo.create", &fpm.BizParam{
		"collection": "customer",
		"row": map[string]interface{}{
			"companymobile": "13770683580",
		},
	})

	assert.Nil(t, err, "should not err")

	assert.NotNil(t, data, "should not nil")
}

//5f86b30c279d8349265339fd

func TestRemove(t *testing.T) {
	fpmApp := fpm.New()

	fpmApp.Init()

	data, err := fpmApp.Execute("mongo.remove", &fpm.BizParam{
		"collection": "customer",
		"id":         "5f86b30c279d8349265339fd",
	})

	assert.Nil(t, err, "should not err")

	assert.NotNil(t, data, "should not nil")
}

func TestUpdate(t *testing.T) {
	fpmApp := fpm.New()

	fpmApp.Init()

	data, err := fpmApp.Execute("mongo.update", &fpm.BizParam{
		"collection": "customer",
		"condition": map[string]interface{}{
			"companymobile": "13770683580",
		},
		"row": map[string]interface{}{
			"companymobile": "13770683582",
		},
	})

	assert.Nil(t, err, "should not err")

	assert.NotNil(t, data, "should not nil")
}

func TestClean(t *testing.T) {
	fpmApp := fpm.New()

	fpmApp.Init()

	data, err := fpmApp.Execute("mongo.clean", &fpm.BizParam{
		"collection": "customer",
		"condition": map[string]interface{}{
			"companymobile": "13770683582",
		},
	})

	assert.Nil(t, err, "should not err")

	assert.NotNil(t, data, "should not nil")
}
