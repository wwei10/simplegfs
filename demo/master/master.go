package main

import (
  "github.com/wweiw/simplegfs"
  log "github.com/Sirupsen/logrus"
  "time"
)

func main() {
  log.SetLevel(log.DebugLevel)
  simplegfs.StartFresh = false
  ms := simplegfs.StartMasterServer(":4444", []string{":5555", ":6666", ":7777"})

  for {
    time.Sleep(time.Second)
  }

  ms.Kill()
}
