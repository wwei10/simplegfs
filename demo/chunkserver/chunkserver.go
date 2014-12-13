package main

import (
  "bufio"
  "github.com/wweiw/simplegfs"
  log "github.com/Sirupsen/logrus"
  "os"
)

func main() {
  simplegfs.StartFresh = false
  if len(os.Args) <= 1 {
    return
  }
  log.SetLevel(log.InfoLevel)
  dir := "/var/tmp/ck" + os.Args[1]
  os.Mkdir(dir, 0777)
  cs := simplegfs.StartChunkServer(":4444", os.Args[1], dir)
  scanner := bufio.NewScanner(os.Stdin)
  for scanner.Scan() {
    line := scanner.Text()
    if line == "quit" {
      break
    }
  }
  cs.Kill()
}
