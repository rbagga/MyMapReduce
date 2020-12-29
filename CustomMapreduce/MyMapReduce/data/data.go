package data 

type MessagePacket struct {

	MessageType string           `json:"type"`

	// {IP address: [heartbeat counter, time, isSuspected, id]}
	Table       map[string][]int `json:"table"`
	Value       []string         `json:"value"`
	SenderIP    string           `json:"senderip"`
}

type FileMeta struct {
        // {Filename: list of IP where file exists}
        Table map[string][]string
}


