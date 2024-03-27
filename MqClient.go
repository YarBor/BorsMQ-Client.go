package BorsMQ_Client_go

import (
	"MqClient/common"
	"context"
	"errors"
	"fmt"
	"github.com/YarBor/BorsMqServer/Random"
	"github.com/YarBor/BorsMqServer/api"
	"google.golang.org/grpc"
	"log"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type MsgData struct {
	T, P string
	Data [][]byte
	Ack  func()
}

type Consumer_ClientEnd interface {
	Pull() (*MsgData, error)
}

type Manager interface {
	LeaveAndJoinGroup(string, registerConsumerGroupOption) error
	FollowTopic(string) error
	UnfollowTopic(string) error
	ConnClose()
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
	t           string
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
	ConsumerInstance_mode_Uncheck  int32 = 0
	ConsumerInstance_mode_Working  int32 = 1
	ConsumerInstance_mode_updating int32 = 2
)

type consumerInstance struct {
	mode            int32
	Self            *api.Credentials
	Group           *api.Credentials
	register        *link
	Key             string
	ConsumerID      string
	ConsumerGroupID string
	wg              sync.WaitGroup
	IsStop          bool

	mu          sync.Mutex
	term        int32
	followPart  map[string]*partition
	getPartFunc func() *partition

	maxWindowSize       int32
	timeoutSessions_ms  int32
	GetEntryNumEachTime int32
	MsgChan             chan *MsgData
}

func (c *consumerInstance) LeaveAndJoinGroup(s string, mode registerConsumerGroupOption) error {
	return c.joinOtherGroup(s, mode)
}

func (c *consumerInstance) FollowTopic(s string) error {
	res, err := c.register.clientEnd.SubscribeTopic(context.Background(), &api.SubscribeTopicRequest{
		CGCred: c.Group,
		Tp:     s,
	})
	if err != nil {
		return err
	} else if res.Response.Mode != api.Response_Success {
		return errors.New(res.Response.Mode.String())
	} else {
		return nil
	}
}

func (c *consumerInstance) UnfollowTopic(s string) error {
	res, err := c.register.clientEnd.UnSubscribeTopic(context.Background(), &api.UnSubscribeTopicRequest{
		CGCred: c.Group,
		Tp:     s,
	})
	if err != nil {
		return err
	} else if res.Response.Mode != api.Response_Success {
		return errors.New(res.Response.Mode.String())
	} else {
		return nil
	}
}

func (c *consumerInstance) ConnClose() {
	c.Stop()
}

func newConsumerInstance() *consumerInstance {
	return &consumerInstance{
		mode:                ConsumerInstance_mode_Uncheck,
		Self:                nil,
		Group:               nil,
		ConsumerID:          "",
		ConsumerGroupID:     "",
		register:            nil,
		Key:                 "",
		wg:                  sync.WaitGroup{},
		IsStop:              false,
		mu:                  sync.Mutex{},
		term:                0,
		followPart:          make(map[string]*partition),
		getPartFunc:         nil,
		maxWindowSize:       common.ConsumerInstance_DefaultMaxWindowSize,
		timeoutSessions_ms:  common.ConsumerInstance_DefaultTimeoutSessions_ms,
		GetEntryNumEachTime: common.ConsumerInstance_DefaultGetEntryNumEachTime,
		MsgChan:             make(chan *MsgData),
	}
}

func (c *consumerInstance) Stop() {
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

func (c *consumerInstance) newGetPartFunc() {
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

func (c *consumerInstance) Pull() (*MsgData, error) {
	return c.PullwithNum(c.GetEntryNumEachTime)
}

func (c *consumerInstance) PullwithNum(GetEntryNum int32) (*MsgData, error) {
	if atomic.LoadInt32(&c.mode) == ConsumerInstance_mode_Uncheck {
		return nil, errors.New("Need To Build Consumer Instance ")
	}
start:
	getPart := c.getPartFunc() // 获取一个分区
	if getPart == nil {        // 如果获取到的分区为空
		c.newGetPartFunc()        // 重新生成getPartFunc闭包函数
		getPart = c.getPartFunc() // 获取新生成的分区
		if getPart == nil {       // 如果仍然获取不到分区
			return nil, fmt.Errorf("There is no partition with topic belong this instance %s", getPart.t)
		}
	}
	getPart.signPull(GetEntryNum) // 向获取到的分区发送消息数目
	data, ok := <-c.MsgChan       // 从消息通道接收数据
	if ok {                       // 如果接收到数据
		if data == nil { // 如果接收到的数据为空 // means partition is empty or partition will be deleted
			goto start // 跳转到标签start，重新获取分区并继续执行
		}
		return data, nil // 返回接收到的数据
	} else { // 如果通道已关闭
		panic(fmt.Sprintf("Partition [%s-%s] is not available", getPart.t, getPart.p))
	}
}

func (c *consumerInstance) registerPartition(p string, members ...struct{ ID, Url string }) error {
	if atomic.LoadInt32(&c.mode) == ConsumerInstance_mode_Uncheck {
		return errors.New("Need To Build Consumer Instance ")
	}
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

// goPullHeartbeat 是 consumerInstance 结构体的方法，用于从指定分区拉取消息。
// 它使用了一个计时器来定期拉取消息，直到 consumerInstance 停止或者接收到停止信号为止。
func (c *consumerInstance) goPullHeartbeat(p *partition) {
	// 在函数退出时标记 WaitGroup 为完成
	defer c.wg.Done()

	// 创建一个立即触发的计时器
	timer := time.NewTimer(0)

	// 循环直到 consumerInstance 停止
	for !c.IsStop {
		// 标记是否计时器超时
		Timeout := false

		// 获取当前的 term
		c.mu.Lock()
		term := c.term
		c.mu.Unlock()

		// 重置计时器以在指定的超时时间后触发
		timer.Reset(time.Millisecond * time.Duration(c.timeoutSessions_ms/3*2))

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
		res, err := p.Pull(c.Group, c.Self, term, num, p.t, p.p)
		if err != nil {
			log.Printf("Error pulling messages: %v", err)
		}

		switch res.Response.Mode {
		case api.Response_Success:
			// 拉取成功
			if res.Msgs.Message != nil && len(res.Msgs.Message) != 0 {
				// 收到消息，发送到 MsgChan，并在确认后更新 commitIndex
				c.MsgChan <- &MsgData{
					T:    p.t,
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
			c.UpdateConsumerGroupFollowPartition()
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
func (c *consumerInstance) UpdateConsumerGroupFollowPartition() {
	if atomic.LoadInt32(&c.mode) == ConsumerInstance_mode_Uncheck {
		log.Print(errors.New("Need To Build Consumer Instance ").Error())
		return
	}
	// 检查并尝试将 consumerInstance 的模式从正常模式切换到更新模式
	if atomic.CompareAndSwapInt32(&c.mode, ConsumerInstance_mode_Working, ConsumerInstance_mode_updating) {
		// 增加 WaitGroup 的计数，以确保在函数结束时减少计数
		c.wg.Add(1)

		// 在新的 goroutine 中执行更新操作
		go func() {
			// 在函数退出时将 consumerInstance 的模式恢复为正常模式
			defer atomic.StoreInt32(&c.mode, ConsumerInstance_mode_Working)

			// 获取连接和释放连接的函数
			link := c.register

			// 调用远程服务检查源端信息
			i, RpcErr := link.clientEnd.CheckSourceTerm(context.Background(), &api.CheckSourceTermRequest{
				Self: c.Group,
				ConsumerData: &api.CheckSourceTermRequest_ConsumerCheck{
					ConsumerId: &c.Self.Id,
					GroupID:    c.Group.Id,
					GroupTerm:  c.term,
				},
			})
			if RpcErr != nil || i.Response.Mode != api.Response_Success {
				// 调用远程服务失败，记录日志并退出更新操作
				log.Printf("Failed to update, remote service call failed: %v", RpcErr)
			}
			// 处理更新结果
			c.handleUpdateRes(i)
		}()
	} else {
		// 如果无法将 consumerInstance 的模式从正常模式切换到更新模式，则直接返回
		return
	}
}

func (c *consumerInstance) handleUpdateRes(res *api.CheckSourceTermResponse) {
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

func (c *consumerInstance) delPartition(part *partition) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.followPart, part.p)
	_ = part.bks.destroy()
	return nil
}

func (c *consumerInstance) feasibility_test() error {
	if atomic.LoadInt32(&c.mode) == ConsumerInstance_mode_Working {
		return nil
	}
	// 检查 Self 字段是否有值
	if c.Self == nil {
		return errors.New("Self credentials not set")
	}

	// 检查 Group 字段是否有值
	if c.Group == nil {
		return errors.New("Group credentials not set")
	}

	// 检查 ConsumerID 字段是否有值
	if c.ConsumerID == "" {
		return errors.New("ConsumerID not set")
	}

	// 检查 ConsumerGroupID 字段是否有值
	if c.ConsumerGroupID == "" {
		return errors.New("ConsumerGroupID not set")
	}

	// 检查 getPartFunc 字段是否有值
	if c.getPartFunc == nil {
		return errors.New("GetPartFunc not set")
	}

	// 检查 MsgChan 字段是否有值
	if c.MsgChan == nil {
		return errors.New("MsgChan not set")
	}

	// 如果所有字段都有值，则返回 nil
	atomic.StoreInt32(&c.mode, ConsumerInstance_mode_Working)
	return nil
}

// todo
// 注册逻辑

func (c *consumerInstance) setConsumerID(ConsumerID string) error {
	if atomic.LoadInt32(&c.mode) != ConsumerInstance_mode_Uncheck {
		return errors.New("consumerInstance is already built")
	}
	c.ConsumerID = ConsumerID
	res, err := c.register.clientEnd.RegisterConsumer(context.Background(), &api.RegisterConsumerRequest{
		MaxReturnMessageSize:    c.maxWindowSize,
		MaxReturnMessageEntries: c.GetEntryNumEachTime,
		TimeoutSessionMsec:      c.timeoutSessions_ms,
	})
	if err != nil {
		return err
	} else {
		if res.Response.Mode != api.Response_Success {
			return errors.New("Register consumer failed with unexpected response mode ")
		}
		c.Self = res.Credential
		c.Key = res.Credential.Key
	}
	return nil
}

type registerConsumerGroupOption int

const (
	RegisterConsumerGroupOption_Latest   registerConsumerGroupOption = 0
	RegisterConsumerGroupOption_Earliest registerConsumerGroupOption = 3
)

func (c *consumerInstance) setConsumerGroupID(ConsumerGroupID string, mode_ifNonexistent registerConsumerGroupOption) error {
	if atomic.LoadInt32(&c.mode) != ConsumerInstance_mode_Uncheck {
		return errors.New("consumerInstance is already built")
	}
	if c.Self == nil {
		err := c.setConsumerID("")
		if err != nil {
			return err
		}
		c.ConsumerGroupID = c.Self.Id
	}
	c.ConsumerGroupID = ConsumerGroupID
	res, err := c.register.clientEnd.RegisterConsumerGroup(context.Background(), &api.RegisterConsumerGroupRequest{
		PullOption: api.RegisterConsumerGroupRequest_PullOptionMode(mode_ifNonexistent),
		GroupId:    &c.ConsumerGroupID,
	})
	if err != nil {
		return err
	} else {
		if res.Response.Mode != api.Response_Success {
			if res.Response.Mode == api.Response_ErrSourceAlreadyExist {
				res, err := c.register.clientEnd.JoinConsumerGroup(context.Background(), &api.JoinConsumerGroupRequest{
					Cred:            c.Self,
					ConsumerGroupId: c.ConsumerGroupID,
				})
				if err != nil || res.Response.Mode != api.Response_Success {
					return err
				} else {
					c.term = res.GroupTerm
					c.Group = &api.Credentials{
						Identity: api.Credentials_ConsumerGroup,
						Id:       c.ConsumerGroupID,
						Key:      c.Self.Key,
					}
					return nil
				}
			}
			return errors.New("Register consumer failed with unexpected response mode ")
		} else {
			c.term = res.GroupTerm
			c.Group = res.Cred
			return nil
		}
	}
}
func (c *consumerInstance) joinOtherGroup(ConsumerGroupID string, mode_ifNonexistent registerConsumerGroupOption) error {
reset:
	switch atomic.LoadInt32(&c.mode) {
	case ConsumerInstance_mode_Uncheck:
		return c.setConsumerGroupID(ConsumerGroupID, mode_ifNonexistent)
	case ConsumerInstance_mode_Working:
		_, err := c.register.clientEnd.LeaveConsumerGroup(context.Background(), &api.LeaveConsumerGroupRequest{
			GroupCred:    c.Group,
			ConsumerCred: c.Self,
		})
		if err != nil {
			return err
		} else {
			return c.setConsumerGroupID(ConsumerGroupID, mode_ifNonexistent)
		}
	case ConsumerInstance_mode_updating:
		time.Sleep(100 * time.Millisecond)
		goto reset
	}

	return nil
}
func (c *consumerInstance) groupFollowTopic(topic string) error {
reset:
	switch atomic.LoadInt32(&c.mode) {
	case ConsumerInstance_mode_Uncheck,
		ConsumerInstance_mode_Working:
		res, err := c.register.clientEnd.SubscribeTopic(context.Background(), &api.SubscribeTopicRequest{
			CGCred: c.Group,
			Tp:     topic,
		})
		if err != nil {
			return err
		} else {
			if res.Response.Mode == api.Response_Success {
				c.UpdateConsumerGroupFollowPartition()
			} else {
				return errors.New(res.Response.Mode.String())
			}
		}
	case ConsumerInstance_mode_updating:
		time.Sleep(100 * time.Millisecond)
		goto reset
	}
	return nil
}
func (c *consumerInstance) groupUnFollowTopic(topic string) error {
reset:
	switch atomic.LoadInt32(&c.mode) {
	case ConsumerInstance_mode_Uncheck,
		ConsumerInstance_mode_Working:
		res, err := c.register.clientEnd.UnSubscribeTopic(context.Background(), &api.UnSubscribeTopicRequest{
			CGCred: c.Group,
			Tp:     topic,
		})
		if err != nil {
			return err
		} else {
			if res.Response.Mode == api.Response_Success {
				c.UpdateConsumerGroupFollowPartition()
			} else {
				return errors.New(res.Response.Mode.String())
			}
		}
	case ConsumerInstance_mode_updating:
		time.Sleep(100 * time.Millisecond)
		goto reset
	}
	return nil
}
func (c *consumerInstance) setTimeOutSession_ms(Session_ms string) error {
	if atomic.LoadInt32(&c.mode) != ConsumerInstance_mode_Uncheck {
		return errors.New("consumerInstance is already built")
	}
	i, err := strconv.Atoi(Session_ms)
	if err != nil {
		return err
	} else {
		if i < 0 {
			return errors.New("Session_ms cannot be negative ")
		}
		c.timeoutSessions_ms = int32(i)
	}
	return nil
}
func (c *consumerInstance) setDefaultGetEntryNum(Num string) error {
	if atomic.LoadInt32(&c.mode) != ConsumerInstance_mode_Uncheck {
		return errors.New("consumerInstance is already built")
	}
	i, err := strconv.Atoi(Num)
	if err != nil {
		return err
	} else {
		if i < 0 {
			return errors.New("DefaultEntryNum cannot be negative ")
		}
		c.timeoutSessions_ms = int32(i)
	}
	return nil
}

func (c *consumerInstance) setMaxWindowSize(Size string) error {
	if atomic.LoadInt32(&c.mode) != ConsumerInstance_mode_Uncheck {
		return errors.New("consumerInstance is already built")
	}
	i, err := strconv.Atoi(Size)
	if err != nil {
		return err
	} else {
		if i < 0 {
			return errors.New("WindowSize cannot be negative ")
		}
		c.maxWindowSize = int32(i)
	}
	return nil
}

func (c *consumerInstance) setKey(Key string) error {
	if atomic.LoadInt32(&c.mode) != ConsumerInstance_mode_Uncheck {
		return errors.New("consumerInstance is already built")
	}
	c.Key = Key
	return nil
}

func (c *consumerInstance) setServer(url string) error {
	if atomic.LoadInt32(&c.mode) != ConsumerInstance_mode_Uncheck {
		return errors.New("consumerInstance is already built")
	}
	l, err := newLink("", url)
	if err != nil {
		return err
	}
	c.register = l
	return nil
}

func (c *consumerInstance) build(options ...ConsumerInstanceOptions) (Consumer_ClientEnd, Manager, error) {
	sort.Slice(options, func(i, j int) bool {
		return options[i].r < options[j].r
	})
	var err error
	for _, option := range options {
		err = option.f(c)
		if err != nil {
			return nil, nil, err
		}
	}

	if c.Group == nil {
		err = c.setConsumerGroupID(Random.RandStringBytes(16), RegisterConsumerGroupOption_Latest)
		if err != nil {
			return nil, nil, err
		}
	}
	err = c.feasibility_test()
	if err != nil {
		return nil, nil, err
	}
	return c, c, nil
}
