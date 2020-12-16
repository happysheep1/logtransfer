package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"

	"test.com/studygo/logtransfer/es"
)

type logData struct {
	data string `json:"data"`
}

//初始化kafka，并且取到数据发送给es
func Init(addrs []string, topic string) (err error) {
	consumer, err := sarama.NewConsumer(addrs, nil)
	if err != nil {
		fmt.Printf("fail to start consumer, err:%v\n", err)
		return
	}
	partitionList, err := consumer.Partitions(topic) // 根据topic取到所有的分区
	if err != nil {
		fmt.Printf("fail to get list of partition:err%v\n", err)
		return
	}
	fmt.Println(partitionList)
	for partition := range partitionList { // 遍历所有的分区
		// 针对每个分区创建一个对应的分区消费者
		pc, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("failed to start consumer for partition %d,err:%v\n", partition, err)
			return err
		}
		//defer pc.AsyncClose()
		// 异步从每个分区消费信息
		go func(sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				//这里应该得发给es
				//var lg = new(logData)
				//err = json.Unmarshal(msg.Value, lg)

				//lg := map[string]interface{}{
				//	"data": string(msg.Value),
				//}
				if err != nil {
					fmt.Println("unmarshal error", err)
					return
				}
				//err = es.Send2es(topic, lg)
				////函数调函数，很low，因为代码顺序变成同步
				////使用chan来变成异步
				//if err != nil {
				//	fmt.Println("Send2es error", err)
				//	return
				//}
				lg := es.LogChanData{Topic: topic, Data: string(msg.Value)}
				es.SendLogChanData2chan(&lg)
				fmt.Printf("Partition:%d Offset:%d Key:%v Value:%v", msg.Partition, msg.Offset, msg.Key, string(msg.Value))
			}
		}(pc)
	}
	return
}
