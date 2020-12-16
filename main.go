package main

import (
	"fmt"
	"gopkg.in/ini.v1"
	"test.com/studygo/logtransfer/es"
	"test.com/studygo/logtransfer/kafka"

	"test.com/studygo/logtransfer/config"
)

var (
	cfg = new(config.LogTransferCfg)
)

//从kafka取出来发往es

func main() {
	//-1加载配置文件

	//另一种使用形式
	//var cfg config.LogTransferCfg
	//err := ini.MapTo(&cfg, "./config/config.ini")
	err := ini.MapTo(cfg, "./config/config.ini")
	if err != nil {
		fmt.Println("初始化参数失败", err)
		return
	}
	fmt.Println("初始化参数成功", cfg)
	//0.初始化
	//2.初始化es连接client
	err = es.Init(cfg.ESCfg.Address, cfg.ESCfg.ChanMaxSize, cfg.ESCfg.ThreadNum)
	if err != nil {
		fmt.Printf("init es error err:%v", err)
		return
	}
	fmt.Println("init es success")
	//0.1初始化kafka,创建了client消费者
	//通过send2es()发往es
	err = kafka.Init([]string{cfg.KafkaCfg.Address}, cfg.KafkaCfg.Topic)
	if err != nil {
		fmt.Printf("init kafka error err:%v", err)
		return
	}
	fmt.Println("init kafka success")

	select {}
	//1.从kafka取数据

	//	2.发往es
}
