package settings

import (
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"fmt"
)

func Get(filename string) *Config {
	var config Config
	source, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Printf("No config file : %s (%s)\n", filename, err)
	} else {
		err = yaml.Unmarshal(source, &config)
		if err != nil {
			fmt.Printf("YAML Unmarshaling Error : %s\n", err)
		}
	}
	return &config
}

func Set(filename string, data *Config) {
	yamlData, yamlErr := yaml.Marshal(data)
	if yamlErr != nil {
		fmt.Printf("YAML Marshalling Error : %s\n", yamlErr)
	}
	writeErr := ioutil.WriteFile(filename, yamlData, 0)
	if writeErr != nil {
		fmt.Printf("YAML Writeout Error : %s\n", yamlData)
	}
}
