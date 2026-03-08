package settings

type Config struct {
	ClusterMembers []Server `yaml:"cluster_members,omitempty"`
	Username string `yaml:"username,omitempty"`
	Password string `yaml:"password,omitempty"`
	SecurityProtocol string `yaml:"security_protocol,omitempty"`
	AuthMethod string `yaml:"authentication_method,omitempty"`
}

type Server struct {
	Hostname string `yaml:"hostname,omitempty"`
	Port int `yaml:"port,omitempty"`
}
