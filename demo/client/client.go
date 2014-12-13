package main

import (
  "bufio"
  "github.com/wweiw/simplegfs"
  log "github.com/Sirupsen/logrus"
  "os"
  "strconv"
  "strings"
)

func create(c *simplegfs.Client, path string) bool {
  ok := strings.HasPrefix(path, "/")
  if !ok {
    log.Infoln("path", path, "should start with '/'")
    return ok
  }
  ok, err := c.Create(path)
  if err != nil {
    log.Warnln(err)
  }
  if ok {
    log.Infoln("created")
  } else {
    log.Infoln("failed")
  }
  return ok
}

func mkdir(c *simplegfs.Client, path string) bool {
  ok := strings.HasPrefix(path, "/")
  if !ok {
    log.Infoln("path", path, "should start with '/'")
    return ok
  }
  ok, err := c.Mkdir(path)
  if err != nil {
    log.Warnln(err)
  }
  if ok {
    log.Infoln("succeeded")
  } else {
    log.Infoln("failed")
  }
  return ok
}

func list(c *simplegfs.Client, path string) {
  ret, err := c.List(path)
  if err != nil {
    log.Warnln(err)
    return
  }
  log.Infoln(ret)
}

func write(c *simplegfs.Client, path, offset, content string) {
  off, err := strconv.ParseUint(offset, 10, 64)
  if err != nil {
    return
  }
  ok := c.Write(path, off, []byte(content))
  if ok {
    log.Infoln("write succeeded")
  } else {
    log.Infoln("write failed")
  }
}

func recordAppend(c *simplegfs.Client, path, content string) {
  off, err := c.Append(path, []byte(content))
  if err != nil {
    log.Infoln(err)
    return
  }
  log.Infoln("append at offset:", off)
}

func parAppend(path string) {
  c0 := simplegfs.NewClient(":4444")
  defer c0.Stop()
  c1 := simplegfs.NewClient(":4444")
  defer c1.Stop()
  c2 := simplegfs.NewClient(":4444")
  defer c2.Stop()
  go func() {
    for i := 0; i < 10; i++ {
      recordAppend(c0, path, strings.Repeat("1", 50) + "\n")
    }
  }()
  go func() {
    for i := 0; i < 10; i++ {
      recordAppend(c1, path, strings.Repeat("2", 50) + "\n")
    }
  }()
  go func() {
    for i := 0; i < 10; i++ {
      recordAppend(c2, path, strings.Repeat("3", 50) + "\n")
    }
  }()
}

func read(c *simplegfs.Client, path, offset, length string) {
  off, err := strconv.ParseUint(offset, 10, 64)
  if err != nil {
    return
  }
  size, err := strconv.ParseUint(length, 10, 64)
  if err != nil {
    return
  }
  if size < 0 {
    return
  }
  bytes := make([]byte, size)
  n, err := c.Read(path, off, bytes)
  if err != nil {
    log.Warnln("read failed:", err)
    return
  }
  log.Infof("read %v characters: %v", n, string(bytes[:n]))
}

func processCmd(c *simplegfs.Client, line string) {
  tokens := strings.Split(line, " ")
  length := len(tokens)
  if length == 0 {
    return
  }
  switch tokens[0] {
  case "create":
    if length == 2 {
      create(c, tokens[1])
      return
    }
  case "mkdir":
    if length == 2 {
      mkdir(c, tokens[1])
      return
    }
  case "write":
    if length == 4 {
      write(c, tokens[1], tokens[2], tokens[3])
      return
    }
  case "read":
    if length == 4 {
      read(c, tokens[1], tokens[2], tokens[3])
      return
    }
  case "append":
    if length == 3 {
      recordAppend(c, tokens[1], tokens[2])
      return
    }
  case "list":
    if length == 2 {
      list(c, tokens[1])
      return
    }
  case "parAppend":
    if length == 2 {
      parAppend(tokens[1])
      return
    }
  }
  log.Infoln("invalid command")
}

func main() {
  log.SetLevel(log.InfoLevel)
  c := simplegfs.NewClient(":4444")
  defer c.Stop()
  scanner := bufio.NewScanner(os.Stdin)
  for scanner.Scan() {
    line := scanner.Text()
    if line == "quit" {
      break
    } else {
      processCmd(c, line)
    }
  }
}
