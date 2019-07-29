package bpct

import (
	"testing"

	"github.com/zssky/log"
)

func TestNewInstance(t *testing.T) {
	i, err := NewInstance("root", "secret", 3306)
	if err != nil {
		log.Fatal(err)
	}

	defer i.Close()
}

func TestInstance_Check(t *testing.T) {
	i, err := NewInstance("root", "secret", 3306)
	if err != nil {
		log.Fatal(err)
	}
	defer i.Close()

	if err := i.Check(); err != nil {
		log.Fatal(err)
	}
}

func TestInstance_Begin(t *testing.T) {
	i, err := NewInstance("root", "secret", 3306)
	if err != nil {
		log.Fatal(err)
	}
	defer i.Close()

	if err := i.Begin("test.aaa"); err != nil {
		log.Fatal(err)
	}
}

func TestInstance_Commit(t *testing.T) {
	i, err := NewInstance("root", "secret", 3306)
	if err != nil {
		log.Fatal(err)
	}
	defer i.Close()

	if err := i.Begin("test.aaa"); err != nil {
		log.Fatal(err)
	}

	if err := i.Execute("test.aaa", []byte("insert into test.aaa select '1', 'name001', 'address001'")); err != nil {
		log.Fatal(err)
	}

	if err := i.Commit("test.aaa"); err != nil {
		log.Fatal(err)
	}
}

func TestInstance_Execute(t *testing.T) {
	i, err := NewInstance("root", "secret", 3306)
	if err != nil {
		log.Fatal(err)
	}
	defer i.Close()

	if err := i.Begin("test.aaa"); err != nil {
		log.Fatal(err)
	}

	if err := i.Execute("test.aaa", []byte("insert into test.aaa select '2', 'name002', 'address002'")); err != nil {
		log.Fatal(err)
	}
}
