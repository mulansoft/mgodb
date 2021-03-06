package mgodb_test

import (
	"encoding/json"
	"errors"
	"fmt"

	"math/rand"
	"os"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	db "github.com/mulansoft/mgodb"
)

type MinCar struct {
	CarId   int64       `json:"carId" bson:"carId"`
	Name    string      `json:"name" bson:"name"`
	Price   int         `json:"price" bson:"price"`
}

func (m MinCar) CollectionName() string {
	return "car"
}

type Car struct {
	MinCar `json:",inline" bson:",inline"`
	Remark  interface{} `json:"remark" bson:"remark"`
	Updated time.Time   `json:"updated" bson:"updated"`
	Created time.Time   `json:"created" bson:"created"`
}

type CarOverview struct {
	Car `json:",inline" bson:",inline"`
	TotalPrice int `json:"totalPrice" bson:"totalPrice"`
}

func (m *CarOverview) CollectionName() string {
	return "car"
}

type Owner struct {
	OwnerId int64  `json:"ownerId" bson:"ownerId"`
	Name    string `json:"name" bson:"name"`
}

type CarOwner struct {
	OwnerId int64   `json:"ownerId" bson:"ownerId"`
	CarId   int64   `json:"carId" bson:"carId"`
	Cars    []Car   `bson:"cars,omitempty"`
	Owners  []Owner `bson:"owners,omitempty"`
}

func NewCar() *Car {
	obj := new(Car)
	obj.CarId = getUUID()
	return obj
}

func initDatabase() {
	log.Info("test init mongodb")
	mongodb := "mongodb://127.0.0.1:27017/test"
	if env := os.Getenv("MONGODB"); env != "" {
		mongodb = env
	}

	log.Info("mongodb: ", mongodb)
	db.Init(mongodb, 128, 30*time.Second)
}

func getUUID() int64 {
	return int64(rand.Uint32())
}

func TestBsonRemark(t *testing.T) {
	initDatabase()

	// new car with remark
	car := NewCar()
	car.Name = "东风风行"
	car.Price = 80000
	car.Remark = bson.M{
		"remark1": car.CarId,
		"remark2": car.CarId,
	}
	db.Insert(car)

	// find car by remark
	obj := new(Car)
	if err := db.FindOne(obj, bson.M{"remark.remark1": car.CarId}); err != nil {
		t.Error("test bson remark error")
	} else {
		jsonData, _ := json.Marshal(obj)
		t.Log(string(jsonData))
	}
}

func TestAggregate(t *testing.T) {
	initDatabase()
	// new car
	car := new(Car)
	car.CarId = 6330682874475319296
	car.Name = "本田思域"
	car.Price = 120000
	db.Insert(car)

	// new owner
	owner := new(Owner)
	owner.OwnerId = 6330682222932135936
	owner.Name = "Simi"
	db.Insert(owner)

	// car_owner
	co := new(CarOwner)
	co.CarId = car.CarId
	co.OwnerId = owner.OwnerId
	db.Insert(co)

	// aggregate
	pipeline := []bson.M{
		bson.M{"$match": bson.M{"ownerId": owner.OwnerId}},
		bson.M{
			"$lookup": bson.M{
				"from":         "car",
				"localField":   "carId",
				"foreignField": "carId",
				"as":           "cars",
			},
		},
		bson.M{
			"$lookup": bson.M{
				"from":         "owner",
				"localField":   "ownerId",
				"foreignField": "ownerId",
				"as":           "owners",
			},
		},
	}
	resp := make([]*CarOwner, 0)
	err := db.Aggregate(&resp, pipeline)
	if err != nil {
		t.Fatal(err)
	}

	// print resp
	for _, item := range resp {
		fmt.Println(item.Cars[0].Name, item.Owners[0].Name)
	}
}

func TestAggregate2(t *testing.T) {
	initDatabase()
	// new car
	car := new(Car)
	car.CarId = 6330682874475319296
	car.Name = "本田思域"
	car.Price = 120000
	db.Insert(car)

	// aggregate
	pipeline := []bson.M{
		{"$match": bson.M{"name": car.Name}},
		{
			"$group": bson.M{
				"_id":        "",
				"totalPrice":   bson.M{"$sum": "$price"},
			},
		},
	}
	resp := make([]*CarOverview, 0)
	err := db.Aggregate(&resp, pipeline)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(resp)

	for _, item := range resp {
		t.Log("total price: ", item.TotalPrice)
	}
}

func TestGetCollectionName(t *testing.T) {
	name := "car"
	assert.Equal(t, name, db.GetCollectionName(new(Car)))
	assert.Equal(t, name, db.GetCollectionName(new(MinCar)))
	assert.Equal(t, name, db.GetCollectionName([]Car{}))
	assert.Equal(t, name, db.GetCollectionName(make([]Car, 0)))
	assert.Equal(t, name, db.GetCollectionName(make([]MinCar, 0)))
}

func TestCRUD(t *testing.T) {
	initDatabase()

	car := NewCar()
	car.Name = "奔驰"
	car.Price = 100

	// 插入功能
	err := db.Insert(car)
	throwFail(t, err)
	if err != nil {
		throwFail(t, errors.New("id value not equal"))
	}
	t.Logf("insert result, id:%v, car:%v", car.CarId, car)

	// 搜索功能
	car1 := NewCar()
	err = db.FindOne(car1, bson.M{"carId": car.CarId})
	throwFail(t, err)
	t.Logf("find result: %v", car1)

	// 更新功能
	err = db.UpdateOne(car1, bson.M{"carId": car.CarId}, bson.M{"$set": bson.M{"name": "BMW"}})
	throwFail(t, err)
	car2 := NewCar()
	err = db.FindOne(car2, bson.M{"carId": car.CarId})
	throwFail(t, err)
	if car2.Name != "BMW" {
		throwFail(t, errors.New("UpdateOne fail"))
	}
	t.Logf("update result: %v", car2)

	// Upsert功能
	car4 := NewCar()
	car4.CarId = getUUID()
	car4.Name = "吉瑞QQ"
	car4.Price = 15
	err = db.UpsertOne(car4, bson.M{"carId": car4.CarId})
	throwFail(t, err)
	car5 := NewCar()
	err = db.FindOne(car5, bson.M{"carId": car4.CarId})
	throwFail(t, err)
	t.Logf("upsert result: %v", car5)

	// 分页功能
	result := []Car{}
	err = db.Find(&result, bson.M{}, 1, 10, []string{"-created"})
	throwFail(t, err)
	t.Logf("search result: %v", result)
	minCars := make([]MinCar, 0)
	err = db.Find(&minCars, bson.M{}, 1, 10, []string{})
	assert.Equal(t, nil, err)
	assert.NotEqual(t, 0, len(minCars))
	t.Logf("minCars: %v", minCars)

	// 删除功能
	car3 := NewCar()
	err = db.RemoveOne(car3, bson.M{"carId": car.CarId})
	throwFail(t, err)
	err = db.FindOne(car3, bson.M{"carId": car.CarId})
	if err != mgo.ErrNotFound {
		throwFail(t, err)
	}
}

func TestUpsertOne(t *testing.T) {
	initDatabase()
	carName := "宝马X5"

	car1 := NewCar()
	car1.CarId = getUUID()
	car1.Name = carName
	car1.Price = 100
	err := db.UpsertOne(car1, bson.M{"name": carName})
	throwFail(t, err)

	car2 := NewCar()
	car2.CarId = getUUID()
	car2.Name = carName
	car2.Price = 150
	err = db.UpsertOne(car2, bson.M{"name": carName})
	throwFail(t, err)

	car3 := NewCar()
	car3.CarId = getUUID()
	car3.Name = carName
	car3.Price = 1250
	err = db.UpsertOne(car3, bson.M{"name": carName})
	throwFail(t, err)

	count := db.Count(&Car{}, bson.M{"name": carName})
	if count != 1 {
		t.Error("TestUpsertOne error")
	}
}

func TestInsertMany(t *testing.T) {
	initDatabase()

	c1 := new(Car)
	c1.CarId = getUUID()
	c1.Name = "c1"
	c1.Price = 100000
	c2 := new(Car)
	c2.CarId = getUUID()
	c2.Name = "c2"
	c2.Price = 20000
	c3 := new(Car)
	c3.CarId = getUUID()
	c3.Name = "c3"
	c3.Price = 30000

	docs := []interface{}{c1, c2, c3}
	err := db.InsertMany(docs)
	throwFail(t, err)

	query := bson.M{"name": "c3"}
	c3 = new(Car)
	err = db.FindOne(c3, query)
	if err != nil || c3.CarId == 0 {
		throwFail(t, err)
	}
}

func throwFail(t *testing.T, err error) {
	if err != nil {
		info := fmt.Sprintf("\t\nError: %s", err.Error())
		t.Errorf(info)
		t.Fail()
	}
}
