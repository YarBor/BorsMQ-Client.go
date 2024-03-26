package BorsMQ_Client_go

import (
	"context"
	"fmt"
	"github.com/YarBor/BorsMqServer/api"
	"google.golang.org/grpc"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type MsgData struct {
	T, P string
	Data [][]byte
	Ack  func()
}
type ClientEnd interface {
	Pull() (MsgData, error)
	Close()
	Commit() error
}

var servicesMtx = sync.RWMutex{}
var services map[string]struct {
	l         *link
	followNum int32
} //brokerID to link

type link struct {
	id        string
	url       string
	conn      *grpc.ClientConn
	clientEnd api.MqServerCallClient
}

func getLink(ID string) (*link, error) {
	servicesMtx.RLock()
	defer servicesMtx.RUnlock()
	data, ok := services[ID]
	if ok {
		return data.l, nil
	} else {
		return nil, fmt.Errorf("Link Not Exist: %v", ID)
	}
}

func newLink(ID, Url string) (*link, error) {
	servicesMtx.Lock()
	defer servicesMtx.Unlock()
	if l, ok := services[ID]; ok {
		return l.l, nil
	}
	conn, err := grpc.Dial(Url, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	l := link{
		id:        ID,
		url:       Url,
		conn:      conn,
		clientEnd: api.NewMqServerCallClient(conn),
	}
	services[ID] = struct {
		l         *link
		followNum int32
	}{l: &l, followNum: 0}
	return &l, nil
}

func addLinkFollowerNum(ID string, num int) error {
	servicesMtx.Lock()
	defer servicesMtx.Unlock()
	i, ok := services[ID]
	if ok {
		atomic.AddInt32(&i.followNum, int32(num))
		return nil
	} else {
		return fmt.Errorf("NOT FOUND")
	}
}

func removeLinkFollowerNum(ID string, num int) error {
	servicesMtx.Lock()
	defer servicesMtx.Unlock()
	i, ok := services[ID]
	if ok {
		if atomic.AddInt32(&i.followNum, int32(-num)) == 0 {
			delete(services, ID)
		}
		return nil
	} else {
		return fmt.Errorf("NOT FOUND")
	}
}

type brokersGroup struct {
	members     []struct{ ID, Url string }
	activeIndex int32
}

func newBrokersGroup(members ...struct{ ID, Url string }) (*brokersGroup, error) {
	for _, member := range members {
		err := addLinkFollowerNum(member.ID, 1)
		if err != nil {
			_, err = newLink(member.ID, member.Url)
			if err != nil {
				return nil, err
			}
			err = addLinkFollowerNum(member.ID, 1)
		}
	}
	return &brokersGroup{members: members}, nil
}

func (s *brokersGroup) destroy() error {
	for _, member := range s.members {
		_ = removeLinkFollowerNum(member.ID, 1)
	}
	return nil
}

var reTryTimes = 3

func (s *brokersGroup) GetLink() (func() (*link, error), func()) {
	index := atomic.LoadInt32(&s.activeIndex)
	var Len = int32(len(s.members))
	var getManyTimes int32 = -1
	return func() (*link, error) {
			getManyTimes += 1
			if getManyTimes > Len*int32(reTryTimes) {
				return nil, fmt.Errorf("exceeded Retry times ")
			}
			l, err := getLink(s.members[index].ID)
			if err != nil {
				l, err = newLink(s.members[index].ID, s.members[index].Url)
			}
			index = (1 + index) % Len
			return l, err
		}, func() {
			if getManyTimes > 0 && getManyTimes <= Len*int32(reTryTimes) {
				atomic.StoreInt32(&s.activeIndex, (index+Len-1)%Len)
			}
		}
}

type partition struct {
	mu          sync.RWMutex // guards
	p           string
	bks         *brokersGroup
	commitIndex int64
	dataRecv    [][]byte
	ch          chan int32
	stopChan    chan struct{}
}

func newPartition(p string, members ...struct {
	ID, Url string
}) (*partition, error) {
	i, err := newBrokersGroup(members...)
	if err != nil {
		return nil, err
	}
	return &partition{
		mu:          sync.RWMutex{},
		p:           p,
		bks:         i,
		commitIndex: 0,
		dataRecv:    nil,
		ch:          make(chan int32),
		stopChan:    make(chan struct{}),
	}, nil
}

func (p *partition) signPull(Entrypoint int32) {
	p.ch <- Entrypoint
}

func (p *partition) Pull(
	Group *api.Credentials,
	Self *api.Credentials,
	GroupTerm int32,
	ReadEntryNum int32,
	Topic string,
	Part string,
) (*api.PullMessageResponse, error) {
	p.mu.RLock()
	f, s := p.bks.GetLink()
	p.mu.RUnlock()
	for {
		l, errGetLink := f()
		if errGetLink != nil {
			log.Printf("Failed to get link for partition Topic=%s, Part=%s: %v", Topic, Part, errGetLink.Error())
			return nil, errGetLink
		}
		res, err := l.clientEnd.PullMessage(context.Background(), &api.PullMessageRequest{
			Group:          Group,
			Self:           Self,
			GroupTerm:      GroupTerm,
			LastTimeOffset: p.commitIndex,
			ReadEntryNum:   ReadEntryNum,
			Topic:          Topic,
			Part:           Part,
		})
		if err != nil {
			log.Printf("Failed to pull messages from partition Topic=%s, Part=%s: %v", Topic, Part, err.Error())
			continue
		}
		if res.Response.Mode == api.Response_Success || res.Response.Mode == api.Response_ErrPartitionChanged || res.Response.Mode == api.Response_ErrSourceNotExist {
			p.mu.Lock()
			p.dataRecv = res.Msgs.Message
			p.mu.Unlock()
			s()
			return res, nil
		} else {
			log.Printf("Received an unexpected response mode for partition Topic=%s, Part=%s: %v", Topic, Part, res.Response.Mode.String())
		}
	}
}

const (
	ConsumerInstance_mode_normal   int32 = 0
	ConsumerInstance_mode_updating int32 = 1
	ConsumerInstance_mode_close    int32 = 2
)

type ConsumerInstance struct {
	mode            int32
	Self            *api.Credentials
	Group           *api.Credentials
	ConsumerID      string
	ConsumerGroupID string
	followTopic     string
	wg              sync.WaitGroup
	IsStop          bool

	mu          sync.Mutex
	term        int32
	followPart  map[string]*partition
	getPartFunc func() *partition

	maxWindowSize   int32
	timeoutSessions int32
	getEntryNum     int32
	MsgChan         chan *MsgData
}

func newConsumerInstance() *ConsumerInstance {
	return &ConsumerInstance{
		mode:            ConsumerInstance_mode_normal,
		Self:            nil,
		Group:           nil,
		ConsumerID:      "",
		ConsumerGroupID: "",
		followTopic:     "",
		wg:              sync.WaitGroup{},
		IsStop:          false,
		mu:              sync.Mutex{},
		term:            0,
		followPart:      make(map[string]*partition),
		getPartFunc:     nil,
		maxWindowSize:   0,
		timeoutSessions: 0,
		getEntryNum:     0,
		MsgChan:         make(chan *MsgData),
	}
}
func (c *ConsumerInstance) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.IsStop = true
	for _, p := range c.followPart {
		select {
		case p.stopChan <- struct{}{}:
		default:
		}
	}
	c.wg.Wait()
}

func (c *ConsumerInstance) newGetPartFunc() {
	c.mu.Lock()                      // 锁定互斥量，以确保并发安全
	defer c.mu.Unlock()              // 在函数返回后解锁互斥量
	parts := []*string{}             // 创建一个存储分区的切片
	for _, p := range c.followPart { // 遍历c.followPart中的每个元素
		parts = append(parts, &p.p) // 将分区p添加到parts切片中
	}
	sort.Slice(parts, func(i, j int) bool { // 对parts切片进行排序，按照p字段的升序排序
		return *parts[i] < *parts[j]
	})
	var index = -1                      // 创建一个索引变量，用于跟踪当前返回的分区，初始值为-1
	c.getPartFunc = func() *partition { // 创建一个闭包函数，并将其赋值给c.getPartFunc字段
		if len(parts) == 0 { // 如果parts切片的长度为0
			return nil // 返回nil，表示没有可用的分区
		} else {
			index++ // 索引递增，指向下一个分区
		}
		if index >= len(parts) { // 如果索引超出了parts切片的长度
			c.newGetPartFunc()     // 递归调用newGetPartFunc函数，重新生成getPartFunc闭包函数
			return c.getPartFunc() // 返回重新生成的getPartFunc闭包函数的结果
		}
		c.mu.Lock()
		i, ok := c.followPart[*parts[index]] // 查找分区名称对应的分区
		c.mu.Unlock()
		if ok {
			return i // 返回当前索引指向的分区
		} else {
			return c.getPartFunc() // 如果未找到，则递归调用getPartFunc函数，继续查找下一个分区
		}
	}
}

func (c *ConsumerInstance) Pull(numMessage int32) (*MsgData, error) {
start:
	getPart := c.getPartFunc() // 获取一个分区
	if getPart == nil {        // 如果获取到的分区为空
		c.newGetPartFunc()        // 重新生成getPartFunc闭包函数
		getPart = c.getPartFunc() // 获取新生成的分区
		if getPart == nil {       // 如果仍然获取不到分区
			return nil, fmt.Errorf("There is no partition with topic belong this instance %s", c.followTopic)
		}
	}
	getPart.signPull(numMessage) // 向获取到的分区发送消息数目
	data, ok := <-c.MsgChan      // 从消息通道接收数据
	if ok {                      // 如果接收到数据
		if data == nil { // 如果接收到的数据为空 // means partition is empty or partition will be deleted
			goto start // 跳转到标签start，重新获取分区并继续执行
		}
		return data, nil // 返回接收到的数据
	} else { // 如果通道已关闭
		panic(fmt.Sprintf("Partition [%s-%s] is not available", c.followTopic, getPart.p))
	}
}

func (c *ConsumerInstance) registerPartition(p string, members ...struct{ ID, Url string }) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	newPart, ok := c.followPart[p]
	if ok {
		return fmt.Errorf("partiton Already exist ")
	} else {
		a, err := newPartition(p, members...)
		if err != nil {
			return err
		}
		newPart = a
		c.followPart[p] = newPart
	}
	c.wg.Add(1)
	go c.goPullHeartbeat(newPart)
	return nil
}

// goPullHeartbeat 是 ConsumerInstance 结构体的方法，用于从指定分区拉取消息。
// 它使用了一个计时器来定期拉取消息，直到 ConsumerInstance 停止或者接收到停止信号为止。
func (c *ConsumerInstance) goPullHeartbeat(p *partition) {
	// 在函数退出时标记 WaitGroup 为完成
	defer c.wg.Done()

	// 创建一个立即触发的计时器
	timer := time.NewTimer(0)

	// 循环直到 ConsumerInstance 停止
	for !c.IsStop {
		// 标记是否计时器超时
		Timeout := false

		// 获取当前的 term
		c.mu.Lock()
		term := c.term
		c.mu.Unlock()

		// 重置计时器以在指定的超时时间后触发
		timer.Reset(time.Millisecond * time.Duration(c.timeoutSessions/3*2))

		var num int32 = 0
		// 选择执行一个操作：停止信号、从通道接收 num、计时器超时
		select {
		case <-p.stopChan:
			// 收到停止信号，返回退出函数
			return
		case num = <-p.ch:
			// 从通道接收 num
		case <-timer.C:
			// 计时器超时
			Timeout = true
		}

		// 拉取消息
		res, err := p.Pull(c.Group, c.Self, term, num, c.followTopic, p.p)
		if err != nil {
			log.Printf("Error pulling messages: %v", err)
		}

		switch res.Response.Mode {
		case api.Response_Success:
			// 拉取成功
			if res.Msgs.Message != nil && len(res.Msgs.Message) != 0 {
				// 收到消息，发送到 MsgChan，并在确认后更新 commitIndex
				c.MsgChan <- &MsgData{
					T:    c.followTopic,
					P:    p.p,
					Data: res.Msgs.Message,
					Ack: func() {
						p.mu.Lock()
						defer p.mu.Unlock()
						p.commitIndex = res.MessageOffset
					},
				}
			} else {
				// 没有收到消息，检查是否可以删除该分区
				if res.IsCouldToDel {
					err = c.delPartition(p)
					if err != nil {
						log.Printf("Error deleting partition: %v", err)
					}
					return
				}
			}
		case api.Response_ErrPartitionChanged:
			// 分区已更改，更新 ConsumerGroupFollowPartition
			c.UpdateConsumerGroupFollowPartition(p)
		case api.Response_ErrSourceNotExist:
			// 源不存在，删除该分区
			log.Printf("Source does not exist")
			err = c.delPartition(p)
			if err != nil {
				log.Printf("Error deleting partition: %v", err)
			}
			return
		}

		// 如果没有超时，则阻塞等待计时器计时结束
		if !Timeout {
			<-timer.C
		}
	}
}

// UpdateConsumerGroupFollowPartition 用于更新消费者组跟随的分区信息。
func (c *ConsumerInstance) UpdateConsumerGroupFollowPartition(p *partition) {
	// 检查并尝试将 ConsumerInstance 的模式从正常模式切换到更新模式
	if atomic.CompareAndSwapInt32(&c.mode, ConsumerInstance_mode_normal, ConsumerInstance_mode_updating) {
		// 增加 WaitGroup 的计数，以确保在函数结束时减少计数
		c.wg.Add(1)

		// 在新的 goroutine 中执行更新操作
		go func() {
			// 在函数退出时将 ConsumerInstance 的模式恢复为正常模式
			defer atomic.StoreInt32(&c.mode, ConsumerInstance_mode_normal)

			// 获取连接和释放连接的函数
			l, s := p.bks.GetLink()

			// 不断尝试更新操作，直到成功或者出错
			for {
				// 获取连接
				link, err := l()
				if err != nil {
					// 获取连接失败，记录日志并退出更新操作
					log.Printf("Failed to update, unable to get link: %v", err)
					return
				}

				// 调用远程服务检查源端信息
				i, RpcErr := link.clientEnd.CheckSourceTerm(context.Background(), &api.CheckSourceTermRequest{
					Self: c.Group,
					ConsumerData: &api.CheckSourceTermRequest_ConsumerCheck{
						ConsumerId: &c.Self.Id,
						GroupID:    c.Group.Id,
						GroupTerm:  c.term,
					},
				})
				if RpcErr != nil {
					// 调用远程服务失败，记录日志并退出更新操作
					log.Printf("Failed to update, remote service call failed: %v", err)
					return
				}

				// 释放连接
				s()

				// 处理更新结果
				c.handleUpdateRes(i)
			}
		}()
	} else {
		// 如果无法将 ConsumerInstance 的模式从正常模式切换到更新模式，则直接返回
		return
	}
}

func (c *ConsumerInstance) handleUpdateRes(res *api.CheckSourceTermResponse) {
	for _, part := range res.ConsumersData.FcParts {
		var mem = make([]struct{ ID, Url string }, 0)
		for _, broker := range part.Part.Brokers {
			mem = append(mem, struct{ ID, Url string }{ID: broker.Id, Url: broker.Url})
		}
		_ = c.registerPartition(part.Part.PartName, mem...)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.term = res.GroupTerm
}

func (c *ConsumerInstance) delPartition(part *partition) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.followPart, part.p)
	_ = part.bks.destroy()
	return nil
}
