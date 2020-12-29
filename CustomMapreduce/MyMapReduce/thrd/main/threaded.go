package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	Data "../../data"
	Juice "../../juice"
	Maple "../../maple"
	Network "../network"
	L "../sdfs"
)

var introducer = "172.22.158.55:7000" // hardcoded to VM1
var myAddr string = "0"
var left bool = false
var mutex sync.Mutex
var file *os.File
var am_introducer int = 0
var replicas map[string]struct{}
var mapleFileAssignments map[string][]string
var juiceFileAssignments map[string][]string
var sdfs_intermediate_filename_prefix string
var numMaples int
var numMapleAggs int = 0
var mapleAggsIP []string = []string{}
var numMapleAcksNeeded int
var numJuices int
var numJuiceAggs int = 0
var numJuiceAcksNeeded int
var sdfs_dest_filename string
var delete_input int
var start time.Time

var master string = "172.22.158.55:7000" // hardcoded to VM1
var currMaxId int = 0
var machineIDs = map[int]string{
	0: introducer,
}

/* A Simple function to verify error */
func CheckError(err error) {
	if err != nil {
		fmt.Println("Error: ", err)
		os.Exit(0)
	}
}

/* Send Data.MessagePacket */
func SendMemList() {
	for {
		if left {
			leave()
			continue
		} else {
			SendList(0)
			time.Sleep(Network.PERIOD * time.Second)
		}
	}
}

/* Send introducer's list to ipaddr */
func SendIntroducerList(ipaddr string) {
	checkFailures(Network.MemList)
	memJson, _ := json.Marshal(Network.MemList)
	Conn, err := net.Dial("udp", ipaddr)
	CheckError(err)
	if err != nil {
		fmt.Println(err)
	}

	defer Conn.Close()

	_, err_wr := Conn.Write(memJson)

	if err_wr != nil {
		fmt.Println(err_wr)
	}

	if _, ok := Network.MemList.Table[myAddr]; ok {
		Network.MemList.Table[myAddr][0]++
	}
}

/* Helper function for sending Data.MessagePacket */
func SendList(toAll int) {
	checkFailures(Network.MemList)

	for _, k := range getServerIPs(Network.MemList) {
		if k == myAddr {
			continue
		}

		servAddr := k
		Conn, err := net.Dial("udp", servAddr)
		CheckError(err)
		if err != nil {
			fmt.Println(err)
		}

		defer Conn.Close()
		if Network.IsGossipStyle == 0 || toAll == 1 {
			allToAllList := Data.MessagePacket{}
			allToAllList.Table = make(map[string][]int)
			allToAllList.Table[myAddr] = Network.MemList.Table[myAddr]
			memJson, _ := json.Marshal(allToAllList)
			_, err_wr := Conn.Write(memJson)
			if err_wr != nil {
				fmt.Println(err_wr)
			}
		} else {
			memJson, _ := json.Marshal(Network.MemList)
			_, err_wr := Conn.Write(memJson)
			if err_wr != nil {
				fmt.Println(err_wr)
			}
		}
	}
	if _, ok := Network.MemList.Table[myAddr]; ok {
		Network.MemList.Table[myAddr][0]++
	}
}

/* Send message to addr */
func SendMessageToAddr(addr string, message []byte) {
	Conn, err := net.Dial("udp", addr)
	CheckError(err)
	if err != nil {
		fmt.Println(err)
	}

	defer Conn.Close()

	_, err_wr := Conn.Write(message)
	if err_wr != nil {
		fmt.Println(err_wr)
	}
}

/* Send message to all machines */
func SendMessageToAll(message []byte) {
	for addr, _ := range Network.MemList.Table {
		SendMessageToAddr(addr, message)
	}
}

/* Server constantly listening */
func server(ServerConn *net.UDPConn) {
	for {
		if left || len(Network.MemList.Table) == 0 {
			continue
		}
		buf := make([]byte, 1024)
		_, _, err := ServerConn.ReadFromUDP(buf)
		if err != nil {
			fmt.Println(err)
		}

		messageReceived := Data.MessagePacket{}
		buf = bytes.Trim(buf, "\x00")
		err = json.Unmarshal(buf, &messageReceived)
		if err != nil {
			fmt.Println("Error: ", err)
		}

		switch messageReceived.MessageType {

		case Network.LEADER:
			master = messageReceived.Value[0]
			fmt.Fprintf(file, "Master is now: %v\n", master)

		case Network.LIST:
			mutex.Lock()
			if _, ok := messageReceived.Table[myAddr]; ok {
				Network.IsGossipStyle = messageReceived.Table[myAddr][3]
			}
			for i, _ := range Network.MemList.Table {
				Network.MemList.Table[i][3] = Network.IsGossipStyle
			}
			for i, _ := range messageReceived.Table {
				if len(messageReceived.Table[i]) == 5 && len(Network.MemList.Table[i]) == 5 && messageReceived.Table[i][4] != 0 {
					machineIDs[messageReceived.Table[i][4]] = i
				}
			}
			fmt.Fprintf(file, "Style is: %v\n", Network.IsGossipStyle)

			mergeList(Network.MemList, messageReceived)
			checkFailures(Network.MemList)
			mutex.Unlock()

		case Network.MAPLE_FETCH:
			fmt.Println("MAPLE FETCHING")
			var assignmentJson *os.File
			assignmentJson, err = os.Open("assignment.json")
			if err != nil {
				fmt.Println(err)
			}
			defer assignmentJson.Close()
			fileAssignment := make(map[string][]string)

			asBytes, _ := ioutil.ReadAll(assignmentJson)
			json.Unmarshal(asBytes, &fileAssignment)
			fmt.Println(fileAssignment)

			mapleFileAssignments = fileAssignment
			myAssignmentCopy := make([]string, len(fileAssignment[myAddr]))
			copy(myAssignmentCopy, fileAssignment[myAddr])

			if myAddr != master {
				L.WaitingFor(len(fileAssignment[myAddr]), "MAPLE", myAssignmentCopy)

				for _, file := range fileAssignment[myAddr] {
					L.Get(file, file)
				}
			} else {
				Maple.ExecuteMaple(fileAssignment[myAddr])
			}
		case L.EXECMAPLE:
			fmt.Println(mapleFileAssignments)
			fmt.Println("GOT MAPLE ACK, my file:", mapleFileAssignments[myAddr])
			Maple.ExecuteMaple(mapleFileAssignments[myAddr])
			// figure out when we delete all these things that we get locally
		case Network.MAPLE_AGG:
			fmt.Println("GOT A MAPLE AGG ACK FROM: ", messageReceived.SenderIP)
			mapleAggsIP = append(mapleAggsIP, messageReceived.SenderIP)
			fmt.Println("MAPLE AGG LIST NOW")
			fmt.Println(mapleAggsIP)

			numMapleAggs += 1
			if numMapleAggs == numMapleAcksNeeded {
				aggregate1()
				numMapleAggs = 0
				mapleAggsIP = []string{}
			}

		case Network.JUICE_FETCH:
			fmt.Println("JUICE FETCHING")

			var partitionJson *os.File
			partitionJson, err = os.Open("partition.json")
			if err != nil {
				fmt.Println(err)
			}
			defer partitionJson.Close()
			partitions := make(map[string][]string)

			asBytes, _ := ioutil.ReadAll(partitionJson)
			json.Unmarshal(asBytes, &partitions)
			fmt.Println(partitions)

			juiceFileAssignments = partitions
			myAssignmentCopy := make([]string, len(partitions[myAddr]))
			copy(myAssignmentCopy, partitions[myAddr])

			if myAddr != master {
				L.WaitingFor(len(partitions[myAddr]), "JUICE", myAssignmentCopy)

				for _, file := range partitions[myAddr] {
					L.Get(file, file)
				}
			} else {
				Juice.ExecuteJuice(partitions[myAddr])
			}
		case Network.JUICE_AGG:
			fmt.Println("GOT A JUICE AGG ACK")
			numJuiceAggs += 1
			fmt.Println("JUICE AGG COUNT: ", numJuiceAggs)
			if numJuiceAggs == numJuiceAcksNeeded {
				fmt.Println("got All Juices")
				aggregate2()
				numJuiceAggs = 0
			}
		case L.EXECJUICE:
			fmt.Println("GOT JUICE ACK, my file:", juiceFileAssignments[myAddr])
			Juice.ExecuteJuice(juiceFileAssignments[myAddr])
		default:
			go L.ReceiveMessages(messageReceived.MessageType, messageReceived.Value, messageReceived.SenderIP)
		}
	}
}

/* Detect and respond to command-line arguments */
func readStdin(am_introducer int) {
	for {
		reader := bufio.NewReader(os.Stdin)
		text, _ := reader.ReadString('\n')
		s := strings.Fields(text)
		if len(s) == 0 {
			continue
		}
		cmd := s[0]

		if cmd == "switch" {
			mutex.Lock()
			if Network.IsGossipStyle == 1 {
				Network.IsGossipStyle = 0
			} else {
				Network.IsGossipStyle = 1
			}
			for i, _ := range Network.MemList.Table {
				Network.MemList.Table[i][3] = Network.IsGossipStyle
			}
			mutex.Unlock()
			SendList(1)
		} else if cmd == "list" {
			var strs []string
			for ip, _ := range Network.MemList.Table {
				strs = append(strs, ip)
			}
			fmt.Println(strings.Join(strs, ", "))
		} else if cmd == "listself" {
			fmt.Println(myAddr)
		} else if cmd == "join" {
			mutex.Lock()
			left = false
			mutex.Unlock()
			join(am_introducer)
		} else if cmd == "leave" {
			Network.MemList.Table[myAddr][2] = 2
			SendList(0)
			leave()
			mutex.Lock()
			left = true
			fmt.Fprintf(file, "%v left\n", myAddr)
			mutex.Unlock()
		} else if cmd == "put" { // put localfilename sdfsfilename
			L.Put(s[1], s[2])
		} else if cmd == "get" { // get sdfsfilename localfilename
			L.Get(s[1], s[2])
		} else if cmd == "delete" { // delete sdfsfilename
			L.Delete(s[1])
		} else if cmd == "ls" { // ls sdfsfilename
			L.Ls(s[1])
		} else if cmd == "store" {
			L.Store()
		} else if cmd == "maple" {
			start = time.Now()
			var ips []string
			for ip, _ := range Network.MemList.Table {
				ips = append(ips, ip)
			}

			executable := s[1]
			num_maples, _ := strconv.Atoi(s[2])
			numMaples = num_maples
			prefix := s[3]
			sdfs_intermediate_filename_prefix = prefix
			srcdir := s[4]
			lines := 20
			chunks := Maple.Chunkify(srcdir, prefix, lines)
			fmt.Println("made ", len(chunks), " chunks")

			assignment := Maple.AssignFiles(chunks, num_maples, ips, executable)
			fmt.Println(assignment)

			num_actual_maples := 0
			for _, files := range assignment {
				if len(files) != 0 {
					num_actual_maples += 1
				}
			}

			numMapleAcksNeeded = num_actual_maples

			mapleFetchMessage := Data.MessagePacket{}
			mapleFetchMessage.MessageType = Network.MAPLE_FETCH
			mapleFetchMessage.SenderIP = myAddr

			mapleByteArray, _ := json.Marshal(mapleFetchMessage)
			SendMessageToAll(mapleByteArray)

		} else if cmd == "juice" {
			start = time.Now()
			var ips []string
			for ip, _ := range Network.MemList.Table {
				ips = append(ips, ip)
			}

			executable := s[1]
			num_juices, _ := strconv.Atoi(s[2])
			numJuices = num_juices
			prefix := s[3]
			// sdfs_intermediate_filename_prefix = prefix
			sdfs_dest_filename = s[4]
			delete_input, _ = strconv.Atoi(s[5])
			partition_type := s[6]

			if partition_type == "range" {
				assignment := Juice.RangePartitionAssignment(ips, executable, num_juices, prefix)
				fmt.Println(assignment)
				numJuiceAcksNeeded = len(assignment)
			} else {
				assignment := Juice.HashPartitionAssignment(ips, executable, num_juices, prefix)
				fmt.Println(assignment)
				numJuiceAcksNeeded = len(assignment)
			}

			juiceFetchMessage := Data.MessagePacket{}
			juiceFetchMessage.MessageType = Network.JUICE_FETCH
			juiceFetchMessage.SenderIP = myAddr

			juiceByteArray, _ := json.Marshal(juiceFetchMessage)
			SendMessageToAll(juiceByteArray)
		}
	}
}

/* Joining a membership group */
func join(am_introducer int) {
	if am_introducer == 0 {
		if len(Network.MemList.Table) == 0 {
			starterJson := `{"table": {"` + myAddr + `":[0,` + strconv.Itoa(int(time.Now().Unix())) + `,0, 1, 0]}, "type":"LIST", "value":[], "senderip":"` + myAddr + `"}`
			json.Unmarshal([]byte(starterJson), &Network.MemList)
		}

		Conn, err := net.Dial("udp", introducer)
		CheckError(err)

		defer Conn.Close()
		memJson, _ := json.Marshal(Network.MemList)
		_, err = Conn.Write(memJson)
		if err != nil {
			fmt.Println(err)
		}
	}
}

/* Get my IP address */
func getIp() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		os.Stderr.WriteString("Error: " + err.Error() + "\n")
		os.Exit(1)
	}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}

/* Merge and update list accordingly */
func mergeList(myList Data.MessagePacket, clientList Data.MessagePacket) {
	myTable := myList.Table
	for k, v := range clientList.Table {
		if k == myAddr {
			continue
		}
		clientBeat := v[0]
		isSuspect := v[2]
		if val, ok := myTable[k]; ok {
			localBeat := val[0]
			// check if current mem list needs to update heartbeat + local time
			if clientBeat > localBeat {
				newTime := int(time.Now().Unix())
				myTable[k][0] = clientBeat
				myTable[k][1] = newTime
				Network.IsGossipStyle = val[3]
				myTable[k][4] = val[4]
				fmt.Fprintf(file, "Updated 1 machine: %v %v \n", k, myTable[k])
			}
			if myTable[k][4] == 0 && am_introducer == 1 {
				myTable[k][4] = currMaxId
				machineIDs[currMaxId] = k
				currMaxId += 1
			}
		} else if isSuspect == 0 {
			// add a new machine to our list
			newTime := int(time.Now().Unix())
			myTable[k] = []int{clientBeat, newTime, 0, Network.IsGossipStyle, 0}
			fmt.Fprintf(file, "Added another machine: %v %v\n", k, myTable[k])
			if am_introducer != 0 {
				SendIntroducerList(k)
			}
		}
	}
}

func getHighestMachineId() string {
	highestId := 0
	for k, _ := range machineIDs {
		if k > highestId {
			highestId = k
		}
	}
	return machineIDs[highestId]
}

/* Check if machine failed */
func checkFailures(myList Data.MessagePacket) {
	myTable := myList.Table
	currentTime := int(time.Now().Unix())
	for k, v := range myTable {
		//k is machine ip
		if k == myAddr {
			continue
		}
		lastUpdateTime := v[1]
		timeDiff := currentTime - lastUpdateTime
		if (timeDiff > Network.CLEANUP && v[2] == 1) || v[2] == 2 {
			delete(machineIDs, v[4])
			delete(myTable, k)
			fmt.Fprintf(file, "Deleted machine from list: %v%v\n", k, myTable[k])

			// elect a new master (highest id) and tell everyone
			if k == master {
				master = getHighestMachineId()
				Network.MemList.MessageType = Network.LEADER
				Network.MemList.Value = []string{master}
				memJson, _ := json.Marshal(Network.MemList)
				SendMessageToAll(memJson)
				L.ChangeMaster(master)
			}

			// check if it is one of my replicas, if so choose another to replicate on
			if _, ok := replicas[k]; ok {
				delete(replicas, k)
				// choose another replica and then onboard
				new_replica := getNewReplicaIP()
				replicas[new_replica] = struct{}{}
				L.OnboardReplica(myAddr, new_replica, k)
			}

			if myAddr == master {
				taskDone := 0
				for _, elem := range mapleAggsIP {
					if elem == k {
						taskDone = 1
						break
					}
				}

				if taskDone != 1 {
					//reassign tas
					myAssignmentCopy := make([]string, len(mapleFileAssignments[myAddr]))
					copy(myAssignmentCopy, mapleFileAssignments[myAddr])
					L.WaitingFor(len(mapleFileAssignments[myAddr]), "MAPLE", myAssignmentCopy)
					for _, file := range mapleFileAssignments[myAddr] {
						L.Get(file, file)
					}

					Maple.ExecuteMaple(mapleFileAssignments[myAddr])

				}
			}

			// add juice failure / maple failure here --> upon detecting a failure, check assignment.json or partition.json depending on what the current task
			// pick another machine to run its tasks on --> call maple_fetch / juice_fetch on that machine
			// check if maple_agg / juice_agg is there for that machine

		} else if timeDiff > Network.TIMEOUT {
			v[2] = 1 //mark as timed out/suspected
			fmt.Fprintf(file, "Marked %v as potentially failed: %v\n", k, myTable[k])
		}
	}
}

/* Determine who to send Data.MessagePacket (based on gossip vs all-to-all) */
func getServerIPs(memlist Data.MessagePacket) []string {
	var servers []string
	for k := range memlist.Table {
		servers = append(servers, k)
	}

	if Network.IsGossipStyle == 1 && len(Network.MemList.Table) > 3 {
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(servers), func(i, j int) { servers[i], servers[j] = servers[j], servers[i] })

		portion := len(servers) / 3
		return servers[:portion]
	} else {
		return servers
	}
}

/* Leave from Data.MessagePackets */
func leave() {
	mutex.Lock()
	for i, _ := range Network.MemList.Table {
		if _, ok := Network.MemList.Table[i]; ok {
			delete(Network.MemList.Table, i)
		}
	}
	for i, _ := range machineIDs {
		if _, ok := machineIDs[i]; ok {
			delete(machineIDs, i)
		}
	}
	mutex.Unlock()
}

/* Assign replicas for all machines */
func assignReplicas(ServerConn *net.UDPConn) {
	for {
		if len(Network.MemList.Table) > Network.REPLICAS {
			replicas = getReplicaIPs()
			L.Start(myAddr, replicas, ServerConn, file)
			break
		}
	}
}

/* Get a new replica IP that isn't your IP */
func getNewReplicaIP() string {
	var servers []string
	for k := range Network.MemList.Table {
		if _, ok := replicas[k]; ok {
			continue
		}
		if k == myAddr {
			continue
		}
		servers = append(servers, k)
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(servers), func(i, j int) { servers[i], servers[j] = servers[j], servers[i] })
	new_replica := ""
	for _, v := range servers {
		new_replica = v
		break
	}
	return new_replica
}

/* Get all replica IPs */
func getReplicaIPs() map[string]struct{} {
	var servers []string
	for k := range Network.MemList.Table {
		if k == myAddr {
			continue
		}
		servers = append(servers, k)
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(servers), func(i, j int) { servers[i], servers[j] = servers[j], servers[i] })

	replicas := make(map[string]struct{})
	var exists = struct{}{}

	for _, r := range servers[:Network.REPLICAS] {
		replicas[r] = exists
	}
	return replicas
}

/* Aggregate all maple outputs by key */
func aggregate1() {
	keyFiles := make(map[string]struct{})
	for ip, shards := range mapleFileAssignments {
		if len(shards) == 0 {
			continue
		}
		keyLineMap := make(map[string]string)
		outputFile := "output" + ip[0:len(ip)-5]
		data, err := ioutil.ReadFile(outputFile)
		for err != nil {
			data, err = ioutil.ReadFile(outputFile)
		}
		lines := strings.Split(string(data), "\n")
		lines = lines[:len(lines)-1]
		for _, line := range lines {
			key := strings.Split(strings.Split(line, " ")[0], "key=")[1]
			keyLineMap[key] += line + "\n"
		}

		for key, vals := range keyLineMap {
			keyFileName := sdfs_intermediate_filename_prefix + key
			keyFile, err := os.OpenFile(keyFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatal(err)
			}
			if _, err := keyFile.Write([]byte(vals)); err != nil {
				log.Fatal(err)
			}
			if err := keyFile.Close(); err != nil {
				log.Fatal(err)
			}

			if _, ok := keyFiles[keyFileName]; !ok {
				keyFiles[keyFileName] = struct{}{}
			}
		}
		t := time.Now()
		elapsed := t.Sub(start)
		fmt.Println("elapsed time for MAPLE:", elapsed)
	}

	keys := []string{}

	for file := range keyFiles {
		fmt.Println("PUTing key file", file)
		L.Put(file, file)
		keys = append(keys, file)
	}
	Juice.Partition(keys)
}

/* Aggregate all reduce outputs to one destination file */
func aggregate2() {
	fmt.Println("AGGREGATE 2 CALLED")
	for ip, shards := range juiceFileAssignments {
		if len(shards) == 0 {
			continue
		}
		keyLineMap := make(map[string]string)
		reducedFile := "reduced" + ip[0:len(ip)-5]
		data, err := ioutil.ReadFile(reducedFile)
		for err != nil {
			data, err = ioutil.ReadFile(reducedFile)
			// fmt.Println("File reading error", err)
			//return
		}

		lines := strings.Split(string(data), "\n")
		lines = lines[:len(lines)-1]
		for _, line := range lines {
			key := strings.Split(strings.Split(line, " ")[0], "key=")[1]
			keyLineMap[key] += line + "\n"
		}

		for _, vals := range keyLineMap {
			// copy all contents of current file (from maple output) to new file
			f, err := os.OpenFile(sdfs_dest_filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatal(err)
			}
			if _, err := f.Write([]byte(vals)); err != nil {
				log.Fatal(err)
			}
			if err := f.Close(); err != nil {
				log.Fatal(err)
			}
		}
	}
	L.Put(sdfs_dest_filename, sdfs_dest_filename)
	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println("elapsed time for JUICE:", elapsed)

	if delete_input == 1 {
		for _, shard := range mapleFileAssignments {
			for _, file := range shard {
				L.Delete(file)
			}
		}
	}
}

func main() {
	port := os.Args[1]
	am_introducer, _ = strconv.Atoi(os.Args[2])
	myAddr = getIp() + ":" + port
	file, _ = os.Create("log-" + string(myAddr))
	if am_introducer == 1 {
		introducer = myAddr
	}

	starterJson := `{"table": {"` + myAddr + `":[0,` + strconv.Itoa(int(time.Now().Unix())) + `,0, 1, 0]}, "type":"LIST", "value":[], "senderip":"` + myAddr + `"}`
	json.Unmarshal([]byte(starterJson), &Network.MemList)

	go SendMemList()
	go readStdin(am_introducer)

	ServerAddr, err := net.ResolveUDPAddr("udp", myAddr)
	CheckError(err)

	ServerConn, err := net.ListenUDP("udp", ServerAddr)
	CheckError(err)

	defer ServerConn.Close()

	go assignReplicas(ServerConn)

	for {
		server(ServerConn)
	}
}
