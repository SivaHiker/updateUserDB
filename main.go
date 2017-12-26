package main

import (
	"gopkg.in/mgo.v2"
	"fmt"
	"gopkg.in/mgo.v2/bson"
	"strings"
)

var jobs chan UserRecord
var done chan bool
var counter int64
var activeCounter int64
var inactiveCounter int64
var c *mgo.Collection


func main(){
	jobs = make(chan UserRecord, 10000)
	//done = make(chan bool, 1)

	session, err := mgo.Dial("10.15.0.68")
	if err != nil {
		panic(err)
	}
	defer session.Close()

	c = session.DB("userdb").C("users")

	var result UserRecord
	for w := 1; w <= 500; w++ {
		go workerPool()
	}

	find := c.Find(bson.M{})
	items := find.Iter()
	for items.Next(&result) {
		jobs <- result
	}
	<-done
}


func workerPool() {
	for (true) {
		select {
		case result,ok := <-jobs:
			if ok {
				if result.Devices != nil && len(result.Devices)>0 {
					for i := 0; i < len(result.Devices); i++ {
						msisdn := result.Devices[i].Msisdn
						if strings.HasPrefix(msisdn, "+9") {
							msisdn=strings.Replace(msisdn, "+9", "+1", 1)
						} else if strings.HasPrefix(msisdn, "+8") {
							msisdn=strings.Replace(msisdn, "+8", "+2", 1)
						} else if strings.HasPrefix(msisdn, "+8") {
							msisdn=strings.Replace(msisdn, "+7", "+3", 1)
						}
						query := bson.M{"devices.msisdn":result.Devices[i].Msisdn}
						coll := c.Update(query, bson.M{"$set": bson.M{"devices.$.msisdn": msisdn}})
						fmt.Println(coll)
					}
				}
				counter++
				fmt.Println("Total Active User records  --- >", counter)
			}
		case <-done:
			done<-true
		}
	}

}

type UserRecord struct {
	ID       bson.ObjectId 		`bson:"_id,omitempty"`
	Addressbook   struct{}      `json:"addressbook"`
	BackupToken   string        `json:"backup_token"`
	Connect       int           `json:"connect"`
	Country       string        `json:"country"`
	Devices       []DeviceDetails `json:"devices"`
	Gender        string        `json:"gender"`
	Icon          string        `json:"icon"`
	InvitedJoined []interface{} `json:"invited_joined"`
	Invitetoken   string        `json:"invitetoken"`
	Locale        string        `json:"locale"`
	Msisdn        []string      `json:"msisdn"`
	Name          string        `json:"name"`
	PaUID         string        `json:"pa_uid"`
	Referredby    []string      `json:"referredby"`
	RewardToken   string        `json:"reward_token"`
	Status        int           `json:"status"`
	Sus           int           `json:"sus"`
	Uls           int           `json:"uls"`
	Version       int           `json:"version"`
}

type DeviceDetails struct {
		Sound            string      `json:"sound"`
		DevID            string      `json:"dev_id"`
		RegTime          int         `json:"reg_time"`
		Preview          bool        `json:"preview"`
		Staging          bool        `json:"staging"`
		Os               string      `json:"os"`
		DevToken         string      `json:"dev_token"`
		DevVersion       string      `json:"dev_version"`
		Msisdn           string      `json:"msisdn"`
		UpgradeTime      int         `json:"upgrade_time"`
		Pdm              string      `json:"pdm"`
		DevType          string      `json:"dev_type"`
		Token            string      `json:"token"`
		OsVersion        string      `json:"os_version"`
		Resolution       string      `json:"resolution"`
		DeviceKey        interface{} `json:"device_key"`
		LastActivityTime int         `json:"last_activity_time"`
		AppVersion       string      `json:"app_version"`
		DevTokenUpdateTs int         `json:"dev_token_update_ts"`
}