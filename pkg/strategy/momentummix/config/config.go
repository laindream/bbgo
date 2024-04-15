package config

type InfluxDB struct {
	URL   string `json:"url"`
	Token string `json:"token"`
	Org   string `json:"org"`
}
