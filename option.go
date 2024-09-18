package yarMessageQueueC

type optionsRank int
type ConsumerInstanceOptions struct {
	r optionsRank
	f func(*consumerInstance) error
}

const (
	zero optionsRank = iota
	one  optionsRank = iota
	two  optionsRank = iota
)

func ConsumerInstance_WithConsumerID(ConsumerID string) ConsumerInstanceOptions {
	return ConsumerInstanceOptions{
		r: one, f: func(instance *consumerInstance) error {
			return instance.setConsumerID(ConsumerID)
		},
	}
}
func ConsumerInstance_WithConsumerGroupID(ConsumerGroupID string, mode registerConsumerGroupOption) ConsumerInstanceOptions {
	return ConsumerInstanceOptions{
		r: two, f: func(instance *consumerInstance) error {
			return instance.setConsumerGroupID(ConsumerGroupID, mode)
		},
	}
}
func ConsumerInstance_WithmaxWindowSize(maxWindowSize string) ConsumerInstanceOptions {
	return ConsumerInstanceOptions{
		r: zero, f: func(instance *consumerInstance) error {
			return instance.setMaxWindowSize(maxWindowSize)
		},
	}
}
func ConsumerInstance_WithtimeoutSessions(timeoutSessions_ms string) ConsumerInstanceOptions {
	return ConsumerInstanceOptions{
		r: zero, f: func(instance *consumerInstance) error {
			return instance.setTimeOutSession_ms(timeoutSessions_ms)
		},
	}
}
func ConsumerInstance_WithdefaultGetEntryNum(defaultGetEntryNum string) ConsumerInstanceOptions {
	return ConsumerInstanceOptions{
		r: zero, f: func(instance *consumerInstance) error {
			return instance.setDefaultGetEntryNum(defaultGetEntryNum)
		},
	}
}
