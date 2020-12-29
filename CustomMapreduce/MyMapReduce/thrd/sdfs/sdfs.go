package sdfs

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/user"
	"strings"
	"time"

	Data "../../data"
	"github.com/pkg/sftp"
	"github.com/tmc/scp"
	"golang.org/x/crypto/ssh"
)

const (
	QUERY         string = "QUERY"
	LSQUERY       string = "LSQUERY"
	QUERYHIT      string = "QUERYHIT"
	LSQUERYHIT    string = "LSQUERYHIT"
	GET           string = "GET"
	PUT           string = "PUT"
	DELETE        string = "DELETE"
	UPDATE        string = "UPDATE"
	DONEPUT       string = "DONEPUT"
	DONEDELETE    string = "DONEDELETE"
	BLOCK         string = "BLOCK"
	REMOVEREPLICA string = "REMOVEREPLICA"
	EXECMAPLE     string = "EXECMAPLE"
	EXECJUICE     string = "EXECJUICE"
	DONEAGGPUTS   string = "DONEAGGPUTS"
)

/* A Simple function to verify error */
func CheckError(err error) {
	if err != nil {
		fmt.Println("Error: ", err)
		os.Exit(0)
	}
}

const TIMEOUT = 30
const ROOT_PATH = "/cs425-mp3/thrd/main/"

//const ROOT_PATH = "/cs425-mp3/"

var myAddr string = "0"
var myFiles []string
var fileMeta = Data.FileMeta{}

var master = "172.22.158.55:7000" // el ip de la VM1
// var master string = "192.168.0.234:6000" // sams ip
var replicas map[string]struct{}
var memlist = Data.MessagePacket{}
var logFile *os.File
var numWaiting int = 0
var waitingType string
var filesWaiting []string
var taskQueue [][]byte

// "filename": [ip's who are waiting to write to this file (synchronous in PUT and DELETE)]
var beingWritten = make(map[string][]string)

func Start(addr string, starter_replicas map[string]struct{}, ServerConn *net.UDPConn, file *os.File) {
	myAddr = addr
	fmt.Println(myAddr)

	logFile = file

	fileMeta.Table = make(map[string][]string)

	starterJson := `{"table": {}, "type":"LIST", "value":[], "senderip":"` + myAddr + `"}`
	json.Unmarshal([]byte(starterJson), &memlist)

	replicas = starter_replicas
	fmt.Println("MY REPLICAS: ", replicas)

}

func WaitingFor(gets int, wait_type string, files []string) {
	// check for this many gets before calling maple exec
	numWaiting = gets
	fmt.Println("numWaiting is ", numWaiting)
	waitingType = wait_type
	fmt.Println("waitingType is ", waitingType)
	filesWaiting = files
	fmt.Println("filesWaiting is ", filesWaiting)
}

func removeFromList(myList []string, toremove string) []string {
	for i, item := range myList {

		if item == toremove {
			myList[i] = myList[len(myList)-1]
			myList[len(myList)-1] = ""
			myList = myList[:len(myList)-1]
		}
	}
	return myList
}

func OnboardReplica(tocopy string, topaste string, onethatfailed string) {
	if myAddr == tocopy {
		replicas[topaste] = struct{}{}
		fmt.Println(onethatfailed + "Failed, New replica of " + tocopy + " is " + topaste)

		for _, file := range myFiles {
			if _, err := os.Stat(file); os.IsNotExist(err) {

			} else {
				err := copyPathPut(topaste, file, file)
				if err != nil {
					fmt.Println("OnboardReplica FAILED")
					log.Println("OnboardReplica FAILED")
					return
				}
				sendToPeer(REMOVEREPLICA, []string{file, onethatfailed}, master)
			}
		}
	}

	// if onethatfailed is currently writing, then remove that ip from beingWritten
	for _, v := range beingWritten {
		removeFromList(v, onethatfailed)
	}
}

func ChangeMaster(new_master string) {
	master = new_master
	if len(myFiles) != 0 {
		sendToPeer(UPDATE, myFiles, master)
	}
}

func getKeyFile() (key ssh.Signer, err error) {
	file := "sshprivatekey"
	buf, err := ioutil.ReadFile(file)
	if err != nil {
		return
	}

	key, err = ssh.ParsePrivateKey(buf)
	if err != nil {
		return
	}

	return key, err
}

func copyPathGet(ipaddr string, localfilename string, sdfsfilename string) error {
	key, err := getKeyFile()

	if err != nil {
		fmt.Println(err)
		log.Println(err)
		return err
	}

	user, err := user.Current()

	if err != nil {
		fmt.Println(err)
		log.Println(err)
		return err
	}

	config := &ssh.ClientConfig{
		User: user.Username,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(key),
		},

		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	sshipaddr := strings.Split(ipaddr, ":")[0] + ":22"

	client, err := ssh.Dial("tcp", sshipaddr, config)
	if err != nil {
		fmt.Println("Failed to dial: " + err.Error())
		log.Println("Failed to dial: " + err.Error())
		return err
	}

	sftp, err := sftp.NewClient(client)
	if err != nil {
		fmt.Println(err)
		log.Println(err)
		return err
	}

	defer sftp.Close()

	srcPath := user.HomeDir + ROOT_PATH + localfilename
	dstPath := user.HomeDir + ROOT_PATH + sdfsfilename

	// Open the source file
	srcFile, err := sftp.Open(srcPath)
	if err != nil {
		fmt.Println(err)
		log.Println(err)
		return err
	}
	defer srcFile.Close()

	// Create the destination file
	dstFile, err := os.Create(dstPath)
	if err != nil {
		fmt.Println(err)
		log.Println(err)
		return err
	}
	defer dstFile.Close()

	// Copy the file
	srcFile.WriteTo(dstFile)
	fmt.Fprintf(logFile, "GET "+sdfsfilename+" from "+ipaddr+" successfully stored locally as "+localfilename)
	return nil
}

//direct

func copyAbsolutePathPut(ipaddr string, absolutefilename string, sdfsfilename string) error {
	key, err := getKeyFile()
	if err != nil {
		fmt.Println(err)
		log.Println(err)
		return err
	}

	user, err := user.Current()
	if err != nil {
		fmt.Println(err)
		log.Println(err)
		return err
	}

	config := &ssh.ClientConfig{
		User: user.Username,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(key),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	sshipaddr := strings.Split(ipaddr, ":")[0] + ":22"

	client, err := ssh.Dial("tcp", sshipaddr, config)
	if err != nil {
		fmt.Println("Failed to dial: " + err.Error())
		log.Println("Failed to dial: " + err.Error())
		return err
	}

	session, err := client.NewSession()
	if err != nil {
		fmt.Println("Failed to create session: " + err.Error())
		log.Println("Failed to create session: " + err.Error())
		return err
	}

	// ABSOLUTE INSTEAD OF LOCAL
	err = scp.CopyPath(absolutefilename, user.HomeDir+"/cs425-mp3/"+sdfsfilename, session)
	if err != nil {
		fmt.Println("TRIED COPYING FROM: ", absolutefilename)
		fmt.Println("Failed to Copy: " + err.Error())
		log.Println("Failed to Copy: " + err.Error())
		return err
	}

	sendToPeer(DONEPUT, []string{sdfsfilename, ipaddr}, ipaddr)
	sendToPeer(DONEPUT, []string{sdfsfilename, ipaddr}, master)
	fmt.Fprintf(logFile, "PUT "+sdfsfilename+" successfully stored locally")
	defer session.Close()
	return nil
}

func copyPathPut(ipaddr string, localfilename string, sdfsfilename string) error {
	key, err := getKeyFile()
	if err != nil {
		fmt.Println(err)
		log.Println(err)
		return err
	}

	user, err := user.Current()
	if err != nil {
		fmt.Println(err)
		log.Println(err)
		return err
	}

	config := &ssh.ClientConfig{
		User: user.Username,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(key),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	sshipaddr := strings.Split(ipaddr, ":")[0] + ":22"

	client, err := ssh.Dial("tcp", sshipaddr, config)
	if err != nil {
		fmt.Println("Failed to dial: " + err.Error())
		log.Println("Failed to dial: " + err.Error())
		return err
	}

	session, err := client.NewSession()
	if err != nil {
		fmt.Println("Failed to create session: " + err.Error())
		log.Println("Failed to create session: " + err.Error())
		return err
	}

	err = scp.CopyPath(user.HomeDir+ROOT_PATH+localfilename, user.HomeDir+ROOT_PATH+sdfsfilename, session)
	if err != nil {
		fmt.Println("Failed to Copy: " + err.Error())
		log.Println("Failed to Copy: " + err.Error())
		return err
	}

	sendToPeer(DONEPUT, []string{sdfsfilename, ipaddr}, ipaddr)
	sendToPeer(DONEPUT, []string{sdfsfilename, ipaddr}, master)
	fmt.Fprintf(logFile, "PUT "+sdfsfilename+" successfully stored locally")
	defer session.Close()
	return nil
}

func sendBytesMsg(bytes []byte) {
	Conn, err := net.Dial("udp", master)
	CheckError(err)
	defer Conn.Close()
	// memlist.MessageType = message
	// memlist.Value = value
	// memlist.SenderIP = myAddr
	// jsonQueryMessage, _ := json.Marshal(memlist)
	_, err_wr := Conn.Write(bytes)
	if err_wr != nil {
		fmt.Println(err_wr)
	}
}

func sendToPeer(message string, value []string, ipaddr string) {
	Conn, err := net.Dial("udp", ipaddr)
	CheckError(err)
	defer Conn.Close()
	memlist.MessageType = message
	memlist.Value = value
	memlist.SenderIP = myAddr
	jsonQueryMessage, _ := json.Marshal(memlist)
	_, err_wr := Conn.Write(jsonQueryMessage)
	if err_wr != nil {
		fmt.Println(err_wr)
	}
}

func DirectPut(absolutefilepath string, sdfsfilename string) {

	start := time.Now()
	putlist := getSetAsList(replicas)
	putlist = append(putlist, myAddr)
	putlist = append([]string{sdfsfilename}, putlist...)
	sendToPeer(PUT, putlist, master)
	if fileMeta.Table == nil {
		fileMeta.Table = make(map[string][]string)
	}

	fileMeta.Table[sdfsfilename] = append(fileMeta.Table[sdfsfilename], myAddr)

	if _, err := os.Stat(absolutefilepath); os.IsNotExist(err) {
		fmt.Println("File does not exist!")
		// CheckError(err)
	} else {
		// go thru replicas and send the local machine file but save it as sdfsfilename
		for ipaddr := range replicas {
			if ipaddr != myAddr {
				err := copyAbsolutePathPut(ipaddr, absolutefilepath, sdfsfilename)
				if err != nil {
					fmt.Println("PUT FAILED!")
					log.Println("PUT FAILED!")
					return
				}
			}
		}

		// duplicate file as sdfsfilename on local
		os.Link(absolutefilepath, sdfsfilename)
		myFiles = append(myFiles, sdfsfilename)
		sendToPeer(DONEPUT, []string{sdfsfilename, myAddr}, master)
		t := time.Now()
		elapsed := t.Sub(start)
		fmt.Println("elapsed time for put:", elapsed)
	}

}

func Put(localfilename string, sdfsfilename string) {

	start := time.Now()
	putlist := getSetAsList(replicas)
	putlist = append(putlist, myAddr)
	putlist = append([]string{sdfsfilename}, putlist...)
	sendToPeer(PUT, putlist, master)
	if fileMeta.Table == nil {
		fileMeta.Table = make(map[string][]string)
	}

	fileMeta.Table[sdfsfilename] = append(fileMeta.Table[sdfsfilename], myAddr)

	if _, err := os.Stat(localfilename); os.IsNotExist(err) {
		fmt.Println("File does not exist!")
		// CheckError(err)
	} else {
		// go thru replicas and send the local machine file but save it as sdfsfilename
		for ipaddr := range replicas {
			if ipaddr != myAddr {
				err := copyPathPut(ipaddr, localfilename, sdfsfilename)
				if err != nil {
					fmt.Println("PUT FAILED!")
					log.Println("PUT FAILED!")
					return
				}
			}
		}

		// duplicate file as sdfsfilename on local
		os.Link(localfilename, sdfsfilename)
		myFiles = append(myFiles, sdfsfilename)
		sendToPeer(DONEPUT, []string{sdfsfilename, myAddr}, master)
		t := time.Now()
		elapsed := t.Sub(start)
		fmt.Println("elapsed time for put:", elapsed)
	}
}

func Get(sdfsfilename string, localfilename string) {
	fmt.Println("ACQUIRING: ", sdfsfilename)
	fmt.Println("STORING LOCALLY TO: ", localfilename)
	_, err := os.Stat(localfilename)
	if !os.IsNotExist(err) && (waitingType == "MAPLE" || waitingType == "JUICE") {
		fmt.Println("already have file", localfilename, "in my directory")
		if _, ok := findElemInList(filesWaiting, localfilename); ok {
			filesWaiting = removeFromList(filesWaiting, localfilename)
			fmt.Println("filesWaiting is now", filesWaiting)
			numWaiting -= 1
			fmt.Println("got one get, waiting for ", numWaiting)
			if numWaiting == 0 {
				fmt.Println("FINSIHED MY GETS for ", waitingType)
				sendToPeer("EXEC"+waitingType, []string{}, myAddr)
			}
		}
		return
	}
	// call QueryHit() -- check if name exists
	sendToPeer(QUERY, []string{sdfsfilename, localfilename}, master)
}

func execGet(sdfsfilename string, localfilename string) {
	start := time.Now()
	iplist := fileMeta.Table[sdfsfilename]
	sender := iplist[0]
	err := copyPathGet(sender, sdfsfilename, localfilename)

	if err != nil {
		fmt.Println("execGet failed1")
		log.Println("execGet failed!")
		return
	}

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println("elapsed time for get:", elapsed)

	fmt.Println("compleeted get for", localfilename, "my waiting files: ", filesWaiting)

	if _, ok := findElemInList(filesWaiting, localfilename); ok {
		filesWaiting = removeFromList(filesWaiting, localfilename)
		fmt.Println("filesWaiting is now", filesWaiting)
		numWaiting -= 1
		fmt.Println("got one get, waiting for ", numWaiting)
		if numWaiting == 0 {
			fmt.Println("FINSIHED MY GETS for ", waitingType)
			sendToPeer("EXEC"+waitingType, []string{}, myAddr)
		}
	}

	// if numWaiting != 0 {
	// 	numWaiting -= 1
	// 	fmt.Println("got one get, waiting for ", numWaiting)
	// 	if numWaiting == 0 {
	// 		fmt.Println("FINSIHED MY GETS")
	// 		sendToPeer(EXECMAPLE, []string{}, myAddr)
	// 	}
	// }
}

func findElemInList(slice []string, val string) (int, bool) {
	for i, item := range slice {
		if item == val {
			return i, true
		}
	}
	return -1, false
}

func Delete(sdfsfilename string) {
	sendToPeer(DELETE, []string{sdfsfilename}, master)
}

func execDelete(sdfsfilename string) {

	if _, err := os.Stat(sdfsfilename); os.IsNotExist(err) {
	} else {
		myFiles = removeFromList(myFiles, sdfsfilename)
		os.Remove(sdfsfilename)
	}

	if myAddr != master {
		sendToPeer(DONEDELETE, []string{sdfsfilename}, master)
		//sendToPeer(DELETE, []string{sdfsfilename}, master)
	}

	var index int = -1
	for i, file := range myFiles {
		if file == sdfsfilename {
			index = i
			break
		}
	}

	if index == -1 {
		return
	}

	myFiles = append(myFiles[:index], myFiles[index+1:]...)
	fmt.Fprintf(logFile, "DELETE "+sdfsfilename+" sucessfully deleted locally")

}

func getListAsSet(myList []string) map[string]struct{} {
	set := make(map[string]struct{})
	for _, item := range myList {
		set[item] = struct{}{}
	}
	return set
}

func getSetAsList(mySet map[string]struct{}) []string {
	myList := make([]string, 0, len(mySet))
	for key, _ := range mySet {
		myList = append(myList, key)
	}
	return myList
}

func Ls(sdfsfilename string) {
	sendToPeer(LSQUERY, []string{sdfsfilename}, master)
}

func executeLs(sdfsfilename string) {
	for ip, _ := range getListAsSet(fileMeta.Table[sdfsfilename]) {
		fmt.Println(ip)
	}

	fmt.Fprintf(logFile, "LS "+sdfsfilename+" listed sucessfully.")
}

func Store() {
	for file, _ := range getListAsSet(myFiles) {
		fmt.Println(file)
	}

	fmt.Fprintf(logFile, "STORE listed sucessfully.")
}

func remove(items []string, item string) []string {
	newitems := []string{}

	for _, i := range items {
		if i != item {
			newitems = append(newitems, i)
		}
	}

	return newitems
}

//like func server from threaded.go
func ReceiveMessages(messageType string, value []string, remoteAddr string) {
	fileName := ""
	if len(value) != 0 {
		fileName = value[0]
	}

	if ips, ok := beingWritten[fileName]; ok {

		if (messageType == DONEPUT || messageType == DONEDELETE) && myAddr == master {
		} else {
			if len(ips) != 0 {
				fmt.Println(remoteAddr, "trying to", messageType, fileName)
				if messageType == QUERY {
					memlist.MessageType = messageType
					memlist.Value = value
					memlist.SenderIP = remoteAddr
					fmt.Println("adding to", myAddr, "quque", memlist)
					jsonQueryMessage, _ := json.Marshal(memlist)
					taskQueue = append(taskQueue, jsonQueryMessage)
				}
				sendToPeer(BLOCK, []string{"CANNOT READ WHILE A WRITE ACTION IS HAPPENING ON THE SAME FILE :)"}, remoteAddr)
				return
			}
		}
	}

	switch messageType {
	case LSQUERY:
		//check meta, return info
		if files, ok := fileMeta.Table[fileName]; ok {
			sendQueryHitMessage(LSQUERYHIT, append([]string{fileName}, files...), remoteAddr)
		}

	case QUERY:
		//check meta, return info
		if files, ok := fileMeta.Table[fileName]; ok {
			sendQueryHitMessage(QUERYHIT, append(value, files...), remoteAddr)
		}

	case LSQUERYHIT:
		addrs := value[1:]
		fileMeta.Table[fileName] = addrs
		executeLs(fileName)

	case QUERYHIT:
		addrs := value[2:]
		fileMeta.Table[fileName] = addrs
		execGet(value[0], value[1])

	case DONEPUT:
		addr := value[1]
		fileMeta.Table[fileName] = append([]string{remoteAddr}, fileMeta.Table[fileName]...)
		fileMeta.Table[fileName] = append([]string{addr}, fileMeta.Table[fileName]...)

		if addr == myAddr {
			myFiles = append(myFiles, fileName)
		}

		// delete from beignwritten
		// beingWritten[fileName] = removeFromList(beingWritten[fileName], remoteAddr)
		for i, item := range beingWritten[fileName] {
			if item == addr {
				beingWritten[fileName] = append(beingWritten[fileName][:i], beingWritten[fileName][i+1:]...)
			}
		}

		for len(taskQueue) > 0 {
			fmt.Println("TAKING ONE OFF OF Q:", taskQueue[0]) // First element
			sendBytesMsg(taskQueue[0])
			taskQueue = taskQueue[1:] // Dequeue
		}

	case PUT:
		beingWritten[fileName] = value[1:]

	case UPDATE:
		fileMeta.Table[fileName] = append([]string{remoteAddr}, fileMeta.Table[fileName]...)

	case DELETE:
		beingWritten[fileName] = append(beingWritten[fileName], myAddr)
		execDelete(fileName)
		beingWritten[fileName] = removeFromList(beingWritten[fileName], myAddr)
		if fileLocations, ok := fileMeta.Table[fileName]; ok {
			for _, ip := range fileLocations {
				sendToPeer(DELETE, []string{fileName}, ip)
				beingWritten[fileName] = append(beingWritten[fileName], ip)
			}
			delete(fileMeta.Table, fileName)
		}

		if len(beingWritten[fileName]) == 1 && beingWritten[fileName][0] == myAddr {
			beingWritten[fileName] = []string{}
		} else if len(beingWritten[fileName]) > 1 {
			beingWritten[fileName] = removeFromList(beingWritten[fileName], myAddr)
		}

	case DONEDELETE:
		if _, ok := fileMeta.Table[fileName]; ok {
			delete(fileMeta.Table, fileName)
		}

		if len(beingWritten[fileName]) == 1 && beingWritten[fileName][0] == remoteAddr {
			beingWritten[fileName] = []string{}
		} else if len(beingWritten[fileName]) > 1 {
			beingWritten[fileName] = remove(beingWritten[fileName], remoteAddr)
		}

	case BLOCK:
		fmt.Println(value[0])

	case REMOVEREPLICA:
		if fileLocations, ok := fileMeta.Table[fileName]; ok {
			for i, ip := range fileLocations {
				if ip == value[1] {
					fileMeta.Table[fileName] = append(fileMeta.Table[fileName][:i], fileMeta.Table[fileName][i+1:]...)
				}
			}
		}

	// ============== END CASES =================

	default:
		return
	}
}

func sendQueryHitMessage(message string, value []string, ipaddr string) {
	Conn, err := net.Dial("udp", ipaddr)
	CheckError(err)
	if err != nil {
		fmt.Println(err)
	}
	defer Conn.Close()
	memlist.MessageType = message
	memlist.Value = value
	queryHitJson, _ := json.Marshal(memlist)
	_, err_wr := Conn.Write(queryHitJson)

	if err_wr != nil {
		fmt.Println(err_wr)
	}
}
