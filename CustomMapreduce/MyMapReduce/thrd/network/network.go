package network

import (
	Data "../../data"
)

const LEADER string = "LEADER"
const LIST string = "LIST"
const EXEC string = "EXEC"
const MAPLE_FETCH string = "MAPLE_FETCH"
const MAPLE_AGG string = "MAPLE_AGG"
const JUICE_FETCH string = "JUICE_FETCH"
const JUICE_AGG string = "JUICE_AGG"

const PERIOD = 1
const TIMEOUT = 10
const CLEANUP = 10
const REPLICAS = 2

var IsGossipStyle int = 1
var MemList = Data.MessagePacket{}
