package util

import (
	"github.com/apache/iotdb-client-go/client"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type conf struct {
	Sg     int32
	Host   string
	Port   string
	User   string
	Pass   string
}

func (c *conf) ReadConf() *conf {
	file, _ := exec.LookPath(os.Args[0])
	path, _ := filepath.Abs(file)
	index := strings.LastIndex(path, string(os.PathSeparator))
	path = path[:index]
	yamlFile, err := ioutil.ReadFile(path + string(os.PathSeparator) + "conf.yaml")
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}

	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}
	config := client.Config{
		Host:     Config.Host,
		Port:     Config.Port,
		UserName: Config.User,
		Password: Config.Pass,
	}
	Session = client.NewSession(&config)
	return c
}