package utils

import (
	"log"

	"github.com/sony/sonyflake"
)

type IdGenerator interface {
	NextId() (int64, error)
}

var _ IdGenerator = (*FlakeIdGenerator)(nil)

type FlakeIdGenerator struct {
	flake *sonyflake.Sonyflake
}

func NewFlakeIdGenerator() FlakeIdGenerator {
	return FlakeIdGenerator{
		flake: sonyflake.NewSonyflake(sonyflake.Settings{}),
	}
}

func (f FlakeIdGenerator) NextId() (int64, error) {
	id, err := f.flake.NextID()
	if err != nil {
		log.Fatalf("flake.NextID() failed with %s\n", err)
		return 0, err
	}
	return int64(id), nil
}
