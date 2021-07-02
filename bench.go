package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"

	pagent "github.com/synerex/proto_people_agent"
	storage "github.com/synerex/proto_storage"
	pb "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"
)

var (
	nodesrv  = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	recv     = flag.Bool("recv", true, "[default] receive data")
	rcount   = flag.Int("rcount", 1, "Simultaneous receiving")
	send     = flag.Bool("send", false, "send data")
	num      = flag.Int("num", 10, "Number of agents for send")
	pace     = flag.Int("pace", 10, "wait for each send [msec]")
	channel  = flag.Int("channel", 0, "force set channel")
	objstore = flag.Bool("objstore", false, "Flag for testing ObjStorage")
	objmbus  = flag.Bool("objmbus", false, "Flag for testing ObjStorage through mbus")
	wait     = flag.Int("wait", 0, "wait seconds before sending")
	count    = flag.Int("count", 1000, "Number of packets for send")
	//	store           = flag.Bool("store", false, "store csv data")
	mu                 sync.Mutex
	version            = "0.02"
	sxServerAddress    string
	startTime, endTime time.Time
	totalPackets       = 0
	totalLength        = 0
	wg                 = &sync.WaitGroup{}
)

func recvCallback(clt *sxutil.SXServiceClient, sp *pb.Supply) {
	if sp.SupplyName == "Agents" {
		if totalPackets == 0 {
			startTime = time.Now()
		}
		agents := &pagent.PAgents{}
		err := proto.Unmarshal(sp.Cdata.Entity, agents)
		if err == nil {
			totalLength += len(sp.Cdata.Entity)
			totalPackets++
		}
	} else if sp.SupplyName == "End" {
		*rcount--
		if *rcount == 0 {
			wg.Done()
		}
		// finish loop!
	}
}

func recvPackets(clt *sxutil.SXServiceClient) {
	ctx := context.Background() //
	err := clt.SubscribeSupply(ctx, recvCallback)
	log.Printf("Error:Supply %s\n", err.Error())
}

func sendPackets(clt *sxutil.SXServiceClient) {
	lat := 35.155162
	lon := 136.966106
	agents := make([]*pagent.PAgent, *num)
	for i := 0; i < *count; i++ {

		for j := 0; j < *num; j++ {
			var latlon = []float64{lon + rand.Float64() - 0.5, lat + rand.Float64() - 0.5}
			agents[j] = &pagent.PAgent{
				Id:    int32(j),
				Point: latlon,
			}
		}
		pagents := pagent.PAgents{
			Agents: agents,
		}

		out, _ := proto.Marshal(&pagents) // TODO: handle error
		totalLength += len(out)
		totalPackets++
		cont := pb.Content{Entity: out}
		smo := sxutil.SupplyOpts{
			Name:  "Agents",
			Cdata: &cont,
		}
		_, nerr := clt.NotifySupply(&smo)
		if nerr != nil { // connection failuer with current client
			log.Printf("Connection failure", nerr)
		}
		if *pace > 0 {
			time.Sleep(time.Millisecond * time.Duration(*pace))
		}
	}
	smo := sxutil.SupplyOpts{
		Name:  "End",
		Cdata: nil,
	}
	clt.NotifySupply(&smo)
}

func objStore(clt *sxutil.SXServiceClient, bc, ob, dt string) {
	//log.Printf("Store %s, %s, %s", bc, ob, dt)
	//  we need to send data into mbusID.
	record := storage.Record{
		BucketName: bc,
		ObjectName: ob,
		Record:     []byte(dt),
		Option:     []byte("raw"),
	}
	out, err := proto.Marshal(&record)
	if err == nil {
		cont := &pb.Content{Entity: out}
		smo := sxutil.SupplyOpts{
			Name:  "Record", // command
			Cdata: cont,
		}
		clt.NotifySupply(&smo)
	}
}

func sendObjStorePackets(clt *sxutil.SXServiceClient) {
	lat := 35.155162
	lon := 136.966106

	// initially start

	agents := make([]*pagent.PAgent, *num)
	for i := 0; i < *count; i++ {

		for j := 0; j < *num; j++ {
			var latlon = []float64{lon + rand.Float64() - 0.5, lat + rand.Float64() - 0.5}
			agents[j] = &pagent.PAgent{
				Id:    int32(j),
				Point: latlon,
			}
		}
		pagents := pagent.PAgents{
			Agents: agents,
		}

		out, _ := proto.Marshal(&pagents) // TODO: handle error
		totalLength += len(out)
		totalPackets++
		cont := pb.Content{Entity: out}
		smo := sxutil.SupplyOpts{
			Name:  "Agents",
			Cdata: &cont,
		}
		_, nerr := clt.NotifySupply(&smo)
		if nerr != nil { // connection failuer with current client
			log.Printf("Connection failure", nerr)
		}
		if *pace > 0 {
			time.Sleep(time.Millisecond * time.Duration(*pace))
		}
	}
	smo := sxutil.SupplyOpts{
		Name:  "End",
		Cdata: nil,
	}
	clt.NotifySupply(&smo)
}

func obsStoreTest(clt *sxutil.SXServiceClient) {
	lat := 35.155162
	lon := 136.966106

	log.Printf("Starting ObjStore benchmark")
	startTime = time.Now()

	for i := 0; i < *count; i++ {
		record := ""
		for j := 0; j < *num; j++ {
			record += fmt.Sprintf("%d, %.3f,%.3f\n", j, lon+rand.Float64()-0.5, lat+rand.Float64()-0.5)
		}
		timeobj := time.Now()
		benchName := fmt.Sprintf("%02d-%02d%02d%02d-%03d", timeobj.Day(), timeobj.Hour(), timeobj.Minute(), timeobj.Second(), i)
		objectName := fmt.Sprintf("bench/%s/%4d/%02d/%02d/%02d/%02d", benchName, timeobj.Year(), timeobj.Month(), timeobj.Day(), timeobj.Hour(), timeobj.Minute())

		sRecord := storage.Record{
			BucketName: "benchmark",
			ObjectName: objectName,
			Record:     []byte(record),
			Option:     []byte("raw"),
		}

		out, _ := proto.Marshal(&sRecord) // TODO: handle error
		totalLength += len(out)
		totalPackets++
		cont := pb.Content{Entity: out}
		smo := sxutil.SupplyOpts{
			Name:  "Record",
			Cdata: &cont,
		}
		_, nerr := clt.NotifySupply(&smo)
		if nerr != nil { // connection failuer with current client
			log.Printf("Connection failure", nerr)
		}
		if *pace > 0 {
			time.Sleep(time.Millisecond * time.Duration(*pace))
		}
	}

	endTime = time.Now()
	duration := endTime.Sub(startTime)
	mins := int(duration.Minutes()) % 60
	secs := int(duration.Seconds()) % 60
	msecs := int(duration.Milliseconds()) % 1000
	log.Printf("Duration: %02d:%02d.%03d\n", mins, secs, msecs)
	log.Printf("Data  %d, %d, %.4f Packet/sec, %.4f Byte/sec", totalPackets, totalLength, float64(totalPackets)/duration.Seconds(), float64(totalLength)/duration.Seconds())

}

func sendRecvTest(clt *sxutil.SXServiceClient) {
	startTime = time.Now()

	if *send { // send mode
		log.Printf("Starting Send Benchmark")
		log.Printf("Count %d, Agent %d, Pace %d", *count, *num, *pace)
		sendPackets(clt)
	} else { // receive mode
		wg.Add(1)
		log.Printf("Starting Receive Benchmark. Wait for sender")
		go recvPackets(clt)
	}
	wg.Wait()
	endTime = time.Now()
	duration := endTime.Sub(startTime)
	mins := int(duration.Minutes()) % 60
	secs := int(duration.Seconds()) % 60
	msecs := int(duration.Milliseconds()) % 1000
	log.Printf("Duration: %02d:%02d.%03d\n", mins, secs, msecs)
	log.Printf("Data  %d, %d, %.4f Packet/sec, %.4f Byte/sec", totalPackets, totalLength, float64(totalPackets)/duration.Seconds(), float64(totalLength)/duration.Seconds())
}

func main() {
	rand.Seed(time.Now().UnixNano())
	flag.Parse()

	go sxutil.HandleSigInt()
	channelTypes := []uint32{pbase.STORAGE_SERVICE}

	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)
	if *objstore {
		channelTypes[0] = pbase.STORAGE_SERVICE
	} else { // agent
		channelTypes[0] = pbase.PEOPLE_AGENT_SVC
	}

	if *channel != 0 {
	   channelTypes[0] = uint32(*channel)
	}
	ndstr := fmt.Sprintf("Bench[%d]",channelTypes[0])

	srv, rerr := sxutil.RegisterNode(*nodesrv, ndstr, channelTypes, nil)
	if rerr != nil {
		log.Fatal("Can't register node ", rerr)
	}
	log.Printf("Connecting SynerexServer at [%s]\n", srv)

	client := sxutil.GrpcConnectServer(srv)
	sxServerAddress = srv
	argJSON := fmt.Sprintf("{Bench}")

	if *wait > 0 {
		time.Sleep(time.Second * time.Duration(*wait))
	}

	//	go subscribeRideSupply(peopleClient)
	//	go monitorStatus() // keep status
	var sclient *sxutil.SXServiceClient
	if *objstore { // for objstore simple test
		sclient = sxutil.NewSXServiceClient(client, channelTypes[0], argJSON)
		obsStoreTest(sclient)
	} else if *objmbus {
		//		sclient = sxutil.NewSXServiceClient(client, pbase.STORAGE_SERVICE, argJSON)
		//		obsStoreMbus()
	} else {
		sclient = sxutil.NewSXServiceClient(client, channelTypes[0], argJSON)
		sendRecvTest(sclient)
	}

	sxutil.UnRegisterNode()
}
