package utils

import (
	"fmt"
	"io/ioutil"
	"os/exec"

	"github.com/zssky/log"
)

// ExeShell shell command without return stdout, stderr, error
func ExeShell(c string) (string, string, error) {
	cmd := exec.Command("/bin/bash", "-c", c)
	op, _ := cmd.StdoutPipe()
	ep, _ := cmd.StderrPipe()
	defer func() {
		if err := op.Close(); err != nil {
			// do nothing
		}

		if err := ep.Close(); err != nil {
			// do nothing
		}
	}()

	// start execute command
	if err := cmd.Start(); err != nil {
		log.Errorf("execute shell command %s error %v", c, err)
		return "", "", err
	}

	orst, err := ioutil.ReadAll(op)
	if err != nil {
		// read stdout pipe error
		log.Errorf("read std out pipe error %v", err)
		return "", "", err
	}

	erst, err := ioutil.ReadAll(ep)
	if err != nil {
		// read error pipe error
		log.Errorf("read error pip error{%v}", err)
		return "", "", err
	}

	// wait for command execute over
	if err := cmd.Wait(); err != nil {
		log.Errorf("shell command out{%s}, err{%s} wait error{%v}", string(orst), string(erst), err)
		return string(orst), string(erst), err
	}

	log.Debugf("output %s, error %s", string(orst), string(erst))
	if len(erst) == 0 {
		return string(orst), string(erst), nil
	}

	return string(orst), string(erst), fmt.Errorf("execute command{%s} error{%s}", c, string(erst))
}
