package es

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	"strings"
	"time"
)

var (
	client *elastic.Client
	//限制异步的管道
	logchan chan *LogChanData
)

//为了使代码变成异步进行封装的结构体
type LogChanData struct {
	Topic string `json:"topic"`
	Data  string `json:"data"`
}

func Init(addr string, chanMaxSize, threadNum int) (err error) {
	if !strings.HasPrefix(addr, "http://") {
		addr = "http://" + addr
	}
	client, err = elastic.NewClient(elastic.SetURL(addr))
	if err != nil {
		// Handle error
		return
	}

	logchan = make(chan *LogChanData, chanMaxSize)
	fmt.Println("connect to es success")

	//开启监听是否有写入管道数据的线程
	for i := 0; i < threadNum; i++ {
		go Send2es()
	}

	return
}

//发送数据到es
func Send2es() {
	for {
		select {
		case msg := <-logchan:
			put1, err := client.Index().
				Index(msg.Topic).
				BodyJson(msg).
				Do(context.Background())
			if err != nil {
				// Handle error
				fmt.Println(err)
				continue
			}
			fmt.Printf("Indexed user %s to index %s, type %s\n", put1.Id, put1.Index, put1.Type)

		default:
			time.Sleep(time.Millisecond * 500)
		}

	}

}

//用来暴露管道的函数
func SendLogChanData2chan(lcd *LogChanData) {
	logchan <- lcd
}
