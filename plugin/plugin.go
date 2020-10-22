package plugin

import (
	"context"

	"github.com/team4yf/yf-fpm-server-go/fpm"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"gopkg.in/mgo.v2/bson"
)

type mongoConfig struct {
	URI  string
	Pool int
	Db   string
}

type queryReq struct {
	Collection string
	Limit      int64

	Sort      string
	Skip      int64
	Condition bson.M
}

type insertReq struct {
	Collection string
	Row        bson.M
}

type batchReq struct {
	Collection string
	Rows       []interface{}
}

type docReq struct {
	Collection string
	ID         string
	Condition  bson.M
	Row        bson.M
}

func init() {
	fpm.RegisterByPlugin(&fpm.Plugin{
		Name: "fpm-plugin-mongo",
		V:    "0.0.1",
		Handler: func(fpmApp *fpm.Fpm) {
			config := mongoConfig{
				URI:  "mongodb://localhost:27017",
				Pool: 1,
				Db:   "test",
			}
			if fpmApp.HasConfig("mongo") {
				if err := fpmApp.FetchConfig("mongo", &config); err != nil {
					panic(err)
				}
			}

			fpmApp.Logger.Debugf("Startup : %s, config: %v", "mongo", config)

			client, err := mongo.NewClient(options.Client().ApplyURI(config.URI))
			if err != nil {
				panic(err)
			}

			ctx := context.Background()
			if err := client.Connect(ctx); err != nil {
				panic(err)
			}

			err = client.Ping(ctx, readpref.Primary())
			if err != nil {
				panic(err)
			}
			db := client.Database(config.Db)

			// export the mongo var
			fpm.Set("MongoClient", client)
			fpm.Set("MongoDb", db)
			fpmApp.AddBizModule("mongo", &fpm.BizModule{
				"find": func(param *fpm.BizParam) (data interface{}, err error) {
					req := queryReq{}
					if err = param.Convert(&req); err != nil {
						return
					}
					fpmApp.Logger.Debugf("req: %#v", req)
					collection := db.Collection(req.Collection)
					list := make([]*bson.M, 0)

					var opts []*options.FindOptions
					if req.Limit > 0 {
						opts = append(opts, &options.FindOptions{
							Limit: &req.Limit,
							Skip:  &req.Skip,
						})
					}
					if req.Sort != "" {
						asc := req.Sort[len(req.Sort)-1:]
						field := req.Sort[0 : len(req.Sort)-1]
						ascOpr := -1
						if asc == "+" {
							ascOpr = 1
						}
						opts = append(opts, &options.FindOptions{
							Sort: bson.M{field: ascOpr},
						})
					}
					cur, err := collection.Find(ctx, req.Condition, opts...)
					if err != nil {
						return nil, err
					}
					defer cur.Close(ctx)
					for cur.Next(ctx) {
						var result bson.M
						if err = cur.Decode(&result); err != nil {
							return
						}
						list = append(list, &result)
					}
					if err = cur.Err(); err != nil {
						return
					}
					data = list
					return
				},
				"first": func(param *fpm.BizParam) (data interface{}, err error) {
					req := queryReq{}
					if err = param.Convert(&req); err != nil {
						return
					}
					fpmApp.Logger.Debugf("req: %#v", req)
					collection := db.Collection(req.Collection)
					one := make(map[string]interface{})
					var opts []*options.FindOneOptions
					if req.Skip > 0 {
						opts = append(opts, &options.FindOneOptions{
							Skip: &req.Skip,
						})
					}
					if req.Sort != "" {
						asc := req.Sort[len(req.Sort)-1:]
						field := req.Sort[0 : len(req.Sort)-1]
						ascOpr := -1
						if asc == "+" {
							ascOpr = 1
						}
						opts = append(opts, &options.FindOneOptions{
							Sort: bson.M{field: ascOpr},
						})
					}
					err = collection.FindOne(ctx, req.Condition, opts...).Decode(&one)
					if err != nil {
						return
					}
					data = one
					return
				},
				"create": func(param *fpm.BizParam) (data interface{}, err error) {
					req := insertReq{}
					if err = param.Convert(&req); err != nil {
						return
					}
					fpmApp.Logger.Debugf("req: %#v", req)
					collection := db.Collection(req.Collection)
					res, err := collection.InsertOne(ctx, req.Row)
					if err != nil {
						return nil, err
					}
					data = res.InsertedID
					return
				},
				"batch": func(param *fpm.BizParam) (data interface{}, err error) {
					req := batchReq{}
					if err = param.Convert(&req); err != nil {
						return
					}
					fpmApp.Logger.Debugf("req: %#v", req)
					collection := db.Collection(req.Collection)
					res, err := collection.InsertMany(ctx, req.Rows)
					if err != nil {
						return nil, err
					}
					data = res.InsertedIDs
					return
				},
				"remove": func(param *fpm.BizParam) (data interface{}, err error) {
					req := docReq{}
					if err = param.Convert(&req); err != nil {
						return
					}
					fpmApp.Logger.Debugf("req: %#v", req)
					collection := db.Collection(req.Collection)
					id, _ := primitive.ObjectIDFromHex(req.ID)
					res, err := collection.DeleteOne(ctx, bson.M{
						"_id": id,
					})
					if err != nil {
						return nil, err
					}
					data = res.DeletedCount
					return
				},
				"save": func(param *fpm.BizParam) (data interface{}, err error) {
					req := docReq{}
					if err = param.Convert(&req); err != nil {
						return
					}
					fpmApp.Logger.Debugf("req: %#v", req)
					collection := db.Collection(req.Collection)
					id, _ := primitive.ObjectIDFromHex(req.ID)
					res, err := collection.UpdateOne(ctx, bson.M{
						"_id": id,
					}, bson.M{
						"$set": req.Row,
					})
					if err != nil {
						return nil, err
					}
					data = res.ModifiedCount
					return
				},
				"update": func(param *fpm.BizParam) (data interface{}, err error) {
					req := docReq{}
					if err = param.Convert(&req); err != nil {
						return
					}
					fpmApp.Logger.Debugf("req: %#v", req)
					collection := db.Collection(req.Collection)
					res, err := collection.UpdateMany(ctx, req.Condition, bson.M{
						"$set": req.Row,
					})
					if err != nil {
						return nil, err
					}
					data = res.ModifiedCount
					return
				},
				"clean": func(param *fpm.BizParam) (data interface{}, err error) {
					req := docReq{}
					if err = param.Convert(&req); err != nil {
						return
					}
					fpmApp.Logger.Debugf("req: %#v", req)
					collection := db.Collection(req.Collection)
					res, err := collection.DeleteMany(ctx, req.Condition)
					if err != nil {
						return nil, err
					}
					data = res.DeletedCount
					return
				},
				"count": func(param *fpm.BizParam) (data interface{}, err error) {
					req := docReq{}
					if err = param.Convert(&req); err != nil {
						return
					}
					fpmApp.Logger.Debugf("req: %#v", req)
					collection := db.Collection(req.Collection)
					data, err = collection.CountDocuments(ctx, req.Condition)
					if err != nil {
						return nil, err
					}
					return
				},
				"findAndCount": func(param *fpm.BizParam) (data interface{}, err error) {
					req := queryReq{}
					if err = param.Convert(&req); err != nil {
						return
					}
					fpmApp.Logger.Debugf("req: %#v", req)
					collection := db.Collection(req.Collection)

					total, err := collection.CountDocuments(ctx, req.Condition)
					if err != nil {
						return nil, err
					}
					list, err := fpmApp.Execute("mongo.find", param)
					if err != nil {
						return nil, err
					}
					data = map[string]interface{}{
						"count": total,
						"rows":  list,
					}
					return
				},
			})

		},
	})
}
