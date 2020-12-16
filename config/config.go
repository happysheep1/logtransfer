package config

type LogTransferCfg struct {
	KafkaCfg `ini:"kafka"`
	ESCfg    `ini:"es"`
}
type KafkaCfg struct {
	Address string `ini:"address"`
	Topic   string `ini:"topic"`
}
type ESCfg struct {
	Address     string `ini:"address"`
	ChanMaxSize int    `ini:"chan_max_size"`
	ThreadNum   int    `ini:"threadnum"`
}
