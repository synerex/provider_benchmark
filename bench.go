package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"
	"math/rand"

	"github.com/golang/protobuf/proto"

	pb "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	pagent "github.com/synerex/proto_people_agent"
	sxutil "github.com/synerex/synerex_sxutil"
)

var (
	nodesrv = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	recv     = flag.Bool("recv", true, "[default] receive data")
	send     = flag.Bool("send", false, "send data")
	num      = flag.Int("num", 10, "Number of agents for send")
	pace     = flag.Int("pace", 10, "wait for each send [msec]")
	count    = flag.Int("count", 1000, "Number of packets for send")
	//	store           = flag.Bool("store", false, "store csv data")
	mu              sync.Mutex
	version         = "0.01"
	sxServerAddress string
	startTime, endTime time.Time
	totalPackets  = 0
	totalLength  = 0 
	wg = &sync.WaitGroup{}
)

func recvCallback(clt *sxutil.SXServiceClient, sp *pb.Supply){
	if sp.SupplyName == "Agents"{
		if totalPackets == 0{
			startTime = time.Now()
		}
		agents := &pagent.PAgents{}
		err := proto.Unmarshal(sp.Cdata.Entity, agents)
		if err == nil {
			totalLength += len(sp.Cdata.Entity)
			totalPackets ++
		}
	}else if sp.SupplyName == "End"{
		wg.Done()
		 // finish loop!
	}
}

func recvPackets(clt *sxutil.SXServiceClient){
	ctx := context.Background() //
	err := clt.SubscribeSupply(ctx, recvCallback)
	log.Printf("Error:Supply %s\n", err.Error())
}

func sendPackets(clt *sxutil.SXServiceClient){
	lat := 35.155162
	lon := 136.966106
	agents := make([]*pagent.PAgent,*num)
	for i := 0; i < *count; i++ {
		
		for j := 0 ; j < *num ; j++ {
			var latlon = []float64{lon+rand.Float64()-0.5, lat+rand.Float64()-0.5}
			agents[j] = & pagent.PAgent{
				Id : int32(j),
				Point: latlon,
			}
		}
		pagents := pagent.PAgents{
			Agents: agents,
		}
		
		out, _ := proto.Marshal(&pagents) // TODO: handle error
		totalLength += len(out)
		totalPackets ++
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
			time.Sleep(time.Millisecond* time.Duration(*pace)  )
		}
	}
	smo := sxutil.SupplyOpts{
		Name:  "End",
		Cdata: nil,
	}
	clt.NotifySupply(&smo)
}


func main() {
	rand.Seed(time.Now().UnixNano())
	flag.Parse()
	
	go sxutil.HandleSigInt()

	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	channelTypes := []uint32{pbase.PEOPLE_AGENT_SVC}
	srv, rerr := sxutil.RegisterNode(*nodesrv, "Bench", channelTypes, nil)
	if rerr != nil {
		log.Fatal("Can't register node ", rerr)
	}
	log.Printf("Connecting SynerexServer at [%s]\n", srv)

	client := sxutil.GrpcConnectServer(srv)
	sxServerAddress = srv
	argJSON := fmt.Sprintf("{Bench}")

	sclient := sxutil.NewSXServiceClient(client, pbase.PEOPLE_AGENT_SVC, argJSON)

	//	go subscribeRideSupply(peopleClient)
//	go monitorStatus() // keep status

	startTime = time.Now()
	if *send { // send mode
		log.Printf("Starting Send Benchmark")
		log.Printf("Count %d, Agent %d, Pace %d", *count, *num , *pace)
		sendPackets(sclient)
	}else{
		wg.Add(1)
		log.Printf("Starting Receive Benchmark. Wait for sender")
		go recvPackets(sclient)
	}
	wg.Wait()
	endTime = time.Now()
	duration := endTime.Sub(startTime)
	mins := int(duration.Minutes()) % 60
    secs := int(duration.Seconds()) % 60
    msecs := int(duration.Milliseconds()) % 1000
    log.Printf("Duration: %02d:%02d.%03d\n", mins, secs,msecs)
	log.Printf("Data  %d, %d, %.4f Packet/sec, %.4f Byte/sec", totalPackets, totalLength, float64(totalPackets)/duration.Seconds(), float64(totalLength)/duration.Seconds())
}
