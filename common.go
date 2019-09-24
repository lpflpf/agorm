package agorm

var MaxTries = 1

func tryAgain(caller func() error) (err error) {
	if err = caller(); err == nil {
		return err
	}

	for i := 0; i < MaxTries; i++ {
		if err = caller(); err == nil {
			break
		}
	}

	return
}
