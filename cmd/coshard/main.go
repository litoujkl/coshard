package main

import (
	"coshard/config"
	"coshard/server"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"gopkg.in/yaml.v2"
)

var (
	coShardConfigFileName = "etc/cs.yaml"

	datanodeConfigFileName = "etc/datanode.json"
	schemaConfigFileName   = "etc/schema.json"
	userConfigFileName     = "etc/user.json"
)

func main()  {
	fmt.Println("---------coshard-----------")
	cfg, err := loadConfig()
	if err != nil {
		panic("load config error")
	}
	server, err := server.NewServer(cfg)
	if err != nil {
	}
	server.Run()
}

func loadConfig() (*config.CoShardConfig, error) {
	nodes, err := parseDataNodes(datanodeConfigFileName)
	if err != nil {
		return nil, err
	}
	nodeMap := make(map[string]config.DataNodeConfig)
	for _, node := range *nodes {
		nodeMap[node.Name] = node
	}

	schemas, err := parseSchemas(schemaConfigFileName)
	if err != nil {
		return nil, err
	}
	schemaMap := make(map[string]config.SchemaConfig)
	for _, schema := range *schemas {
		schemaMap[schema.Name] = schema
	}

	users, err := parseUsers(userConfigFileName)
	if err != nil {
		return nil, err
	}
	userMap := make(map[string]config.UserConfig)
	for _, user := range *users {
		userMap[user.User] = user
	}

	cfg := new(config.CoShardConfig)
	cfg.Nodes = nodeMap
	cfg.Schemas = schemaMap
	cfg.Users = userMap

	err = parseMainConfig(coShardConfigFileName, cfg)
	if err != nil {
		return nil, nil
	}

	return cfg, nil
}

func parseMainConfig(fileName string, cfg *config.CoShardConfig) error {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return err
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return err
	}
	return nil
}

func parseDataNodes(fileName string) (*[]config.DataNodeConfig, error) {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	var cfg []config.DataNodeConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func parseSchemas(fileName string) (*[]config.SchemaConfig, error) {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	var cfg []config.SchemaConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func parseUsers(fileName string) (*[]config.UserConfig, error) {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	var cfg []config.UserConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
