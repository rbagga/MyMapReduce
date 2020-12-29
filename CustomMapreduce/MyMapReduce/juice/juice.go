package juice

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"sort"
	"strings"

	Data "../data"
	Network "../thrd/network"
	SDFS "../thrd/sdfs"
	"github.com/tmc/scp"
	"golang.org/x/crypto/ssh"
)

var master string = "172.22.158.55:7000"
var keyFiles []string
var ips []string

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

/* Send message to a certain IP address */
func sendMessageToAddr(addr string, message []byte) {
	Conn, err := net.Dial("udp", addr)
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
func sendMessageToAll(message []byte) {
	for addr, _ := range Network.MemList.Table {
		sendMessageToAddr(addr, message)
	}
}

/* Get ssh key file */
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

/* PUT using SCP */
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

	err = scp.CopyPath(user.HomeDir+"/cs425-mp3/thrd/main/"+localfilename, user.HomeDir+"/cs425-mp3/thrd/main/"+sdfsfilename, session)
	if err != nil {
		fmt.Println("Failed to Copy: " + err.Error())
		log.Println("Failed to Copy: " + err.Error())
		return err
	}
	return nil
}

/* Execute juice command on each machine */
func ExecuteJuice(shards []string) {
	// get maple_exe --> maybe do this in threaded
	// execute maple on each machine
	outputs := ""
	for _, file := range shards {
		fmt.Println("running command on ", file)
		cmd := exec.Command("go", "run", "juice_exe.go", file)
		var out bytes.Buffer
		cmd.Stdout = &out
		err := cmd.Run()
		if err != nil {
			fmt.Println(err)
		}
		outputs += out.String()
	}
	myAddr := getIp()
	ioutil.WriteFile("reduced"+myAddr, []byte(outputs), 0644)
	copyPathPut(master, "reduced"+myAddr, "reduced"+myAddr)

	mapleAggMessage := Data.MessagePacket{}
	mapleAggMessage.MessageType = "JUICE_AGG"
	mapleAggMessage.SenderIP = myAddr

	mapleByteArray, _ := json.Marshal(mapleAggMessage)
	sendMessageToAddr(master, mapleByteArray)
}

/* Upload assignment of partitions into SDFS */
func uploadAssignment(fileAssignment map[string][]string, executable string) {
	file, _ := json.MarshalIndent(fileAssignment, "", " ")
	_ = ioutil.WriteFile("partition.json", file, 0644)
	for _, ip := range ips {
		copyPathPut(ip, "partition.json", "partition.json")
		copyPathPut(ip, executable, "juice_exe.go")
	}
}

/* Partitioned input */
func Partition(files []string) {
	keyFiles = files
}

/* Set juice information */
func SetJuiceInfo(activeIps []string, executable string, num_juices int, prefix string) map[string][]string {
	ips = activeIps
	fileAssignment := make(map[string][]string)

	for _, ip := range activeIps {
		fileAssignment[ip] = []string{}
	}

	for i, file := range keyFiles {
		ip := activeIps[i%num_juices]
		fileAssignment[ip] = append(fileAssignment[ip], file)
	}

	uploadAssignment(fileAssignment, executable)
	return fileAssignment
}

/* Assignment using range partitioning */
func RangePartitionAssignment(activeIps []string, executable string, num_juices int, prefix string) map[string][]string {
	ips = activeIps
	rangePartition := make(map[string][]string)
	sort.Strings(keyFiles)
	var num_partitions int
	var partitions [][]string

	fmt.Println("KEY FILE COUNT: ", len(keyFiles))
	fmt.Println("NUM JOOZES: ", num_juices)

	// cant partition more than number of files
	if num_juices <= len(keyFiles) {
		num_partitions = num_juices
	} else {
		num_partitions = len(keyFiles)
	}

	partition_size := len(keyFiles) / num_partitions

	fmt.Println(partition_size)

	// source: stackoverflow.com/questions/57524!5/how-to-split-a-slice-in-go-in-sub-slices

	var j int
	for i := 0; i < len(keyFiles); i += partition_size {
		j += partition_size
		if j > len(keyFiles) {
			j = len(keyFiles)
		}
		fmt.Println(keyFiles[i:j])
		partitions = append(partitions, keyFiles[i:j])
	}

	fmt.Println(partitions)
	for i, _ := range partitions {
		ip_select := int(math.Min(float64(i), float64(len(activeIps)-1)))
		ip := activeIps[ip_select]
		rangePartition[ip] = append(rangePartition[ip], partitions[i]...)
	}
	uploadAssignment(rangePartition, executable)
	return rangePartition

}

// source: https://stackoverflow.com/questions/13582519/how-to-generate-hash-number-of-a-string-in-go

/* Hash function */
func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

/* Assignment using hash partitioning */
func HashPartitionAssignment(activeIps []string, executable string, num_juices int, prefix string) map[string][]string {
	hashPartition := make(map[string][]string)

	var partitions int
	if num_juices > len(activeIps) {
		partitions = len(activeIps)
	} else {
		partitions = num_juices
	}

	for _, file := range keyFiles {
		keyName := strings.TrimPrefix(file, prefix)
		ip := activeIps[int(hash(keyName))%partitions]
		hashPartition[ip] = append(hashPartition[ip], file)
	}

	fmt.Println(hashPartition)
	uploadAssignment(hashPartition, executable)
	return hashPartition
}

/* PUT all files into SDFS */
func uploadFiles(mapFiles []string) {
	for _, file := range mapFiles {
		SDFS.Put(file, file)
	}
}

// source: stackoverflow.com/questions/21362950/getting-a-slice-of-keys-from-a-map
/* Get all active IP addresses */
func retrieveActiveIPs() []string {

	ipMap := Network.MemList.Table
	keys := make([]string, 0, len(ipMap))

	return keys

	for k, _ := range ipMap {
		keys = append(keys, k)
	}

	return keys
}

// source: https://flaviocopes.com/go-list-files/
/* Visit file */
func visit(files *[]string) filepath.WalkFunc {
	return func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Fatal(err)
		}
		*files = append(*files, path)
		return nil
	}
}
