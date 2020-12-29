package maple

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"

	Data "../data"
	Network "../thrd/network"
	SDFS "../thrd/sdfs"
	"github.com/tmc/scp"
	"golang.org/x/crypto/ssh"
)

var master string = "172.22.158.55:7000"

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

/* Send message to certain IP address */
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

/* Execute maple command on each machine */
func ExecuteMaple(shards []string) {
	outputs := ""
	for _, file := range shards {
		fmt.Println("running command on ", file)
		cmd := exec.Command("go", "run", "maple_exe.go", file)
		var out bytes.Buffer
		cmd.Stdout = &out
		err := cmd.Run()
		if err != nil {
			fmt.Println(err)
		}
		outputs += out.String()
	}
	myAddr := getIp()
	ioutil.WriteFile("output"+myAddr, []byte(outputs), 0644)
	copyPathPut(master, "output"+myAddr, "output"+myAddr)
	mapleAggMessage := Data.MessagePacket{}
	mapleAggMessage.MessageType = "MAPLE_AGG"
	mapleAggMessage.SenderIP = myAddr

	mapleByteArray, _ := json.Marshal(mapleAggMessage)
	sendMessageToAddr(master, mapleByteArray)
}

func fileExists(filename string) bool {
    _, err := os.Stat(filename)
    if os.IsNotExist(err) {
        return false
    }
    return true
}

/* Partition input into chunks */
func Chunkify(srcdir string, pref string, lines int) []string {

	fmt.Println("retrieving files")
	var allFiles []string
	err := filepath.Walk(srcdir, visit(&allFiles))

	if err != nil {
		fmt.Println(err)
	}

	lines_ := strconv.Itoa(lines)
	dir := allFiles[0]
	allFiles = allFiles[1:]

	for _, file := range allFiles {
		//shard
		cmd := exec.Command("split", "-l", lines_, file, dir+"/"+pref)
		var out bytes.Buffer
		cmd.Stdout = &out
		err := cmd.Run()
		if err != nil {
			fmt.Println(err)
		}
	}
	allChunks := gatherFiles(srcdir, pref)
	return allChunks
}

/* Get all files with certain prefix */
func gatherFiles(srcdir string, pref string) []string {
	var files []string
	var allFiles []string
	err := filepath.Walk(srcdir, visit(&allFiles))
	if err != nil {
		fmt.Println(err)
	}

	for _, file := range allFiles {
		if strings.HasPrefix(file, srcdir+"/"+pref) {
			dest_file := strings.TrimPrefix(file, srcdir+"/")
			SDFS.Put(file, dest_file)
			files = append(files, dest_file)

		}
	}

	return files
}

/* Upload assignment of partitions into SDFS */
func uploadAssignment(fileAssignment map[string][]string, executable string) {
	file, _ := json.MarshalIndent(fileAssignment, "", " ")
	_ = ioutil.WriteFile("assignment.json", file, 0644)
	for ip, _ := range fileAssignment {
		copyPathPut(ip, "assignment.json", "assignment.json")
		copyPathPut(ip, executable, "maple_exe.go")
	}
}

/* Assign partitioned files to worker machines */
func AssignFiles(files []string, num int, activeIps []string, executable string) map[string][]string {

	fileAssignment := make(map[string][]string)

	for _, ip := range activeIps {
		fileAssignment[ip] = []string{}
	}

	for i, file := range files {
		ip := activeIps[i%num]
		fileAssignment[ip] = append(fileAssignment[ip], file)
	}

	uploadAssignment(fileAssignment, executable)
	return fileAssignment
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
