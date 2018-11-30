package allocation

import (
	"io"
	"time"

	nomad "github.com/hashicorp/nomad/api"
)

type Client struct {
	NomadClient *nomad.Client
}

const DefaultPollInterval = 10

const StdErr = "stderr"
const StdOut = "stdout"

func (a *Client) SyncAllocations(nodeID *string, currentAllocations *[]*nomad.Allocation, addedChan chan<- *nomad.Allocation, removedChan chan<- *nomad.Allocation, errChan chan<- error, pollInterval int) {
	if len(*currentAllocations) > 0 {
		nextTime := time.Now().Truncate(time.Second * time.Duration(pollInterval))
		nextTime = nextTime.Add(time.Second * time.Duration(pollInterval))
		time.Sleep(time.Until(nextTime))
	} else {
		time.Sleep(time.Second * 1)
	}

	allocations, err := a.GetAllocationsForNode(nodeID)

	var foundAllocations []*nomad.Allocation

	if err != nil {
		errChan <- err
	} else {
		for _, allocation := range allocations {
			if allocation.ClientStatus == "running" || allocation.ClientStatus == "restarting" {
				foundAllocations = append(foundAllocations, allocation)

				if !allocationInSlice(allocation, *currentAllocations) {
					addedChan <- allocation
				}
			}
		}

		if len(*currentAllocations) > 0 {
			for _, allocation := range *currentAllocations {
				if !allocationInSlice(allocation, foundAllocations) {
					removedChan <- allocation
				}
			}
		}

		*currentAllocations = foundAllocations
	}

	a.SyncAllocations(nodeID, currentAllocations, addedChan, removedChan, errChan, pollInterval)
}

func (a *Client) GetAllocationsForNode(nodeID *string) ([]*nomad.Allocation, error) {
	allocations, _, err := a.NomadClient.Nodes().Allocations(*nodeID, nil)

	return allocations, err
}

func allocationInSlice(item *nomad.Allocation, list []*nomad.Allocation) bool {
	for _, i := range list {
		if i.ID == item.ID {
			return true
		}
	}

	return false
}

func (a *Client) GetAllocationInfo(ID string) (*nomad.Allocation, error) {
	alloc, _, err := a.NomadClient.Allocations().Info(ID, nil)

	return alloc, err
}

func (a *Client) GetLog(logType string, alloc *nomad.Allocation, taskName string, offset int64) *nomad.StreamFrame {
	data, _ := a.NomadClient.AllocFS().Logs(alloc, false, taskName, logType, "start", offset, nil, nil)

	content := <-data

	return content
}

func (a *Client) GetLogSize(logType string, alloc *nomad.Allocation, taskName string, offset int64) int64 {
	frames, errors := a.NomadClient.AllocFS().Logs(alloc, false, taskName, logType, "start", offset, nil, nil)

	size := 0

	var err error
	var n int

	n, err = readStreamFrame(frames, errors)

	size = size + n

	for err == nil {
		n, err = readStreamFrame(frames, errors)
		size = size + n
	}

	return int64(size)
}

func (a *Client) StreamLog(logType string, alloc *nomad.Allocation, taskName string, offset int64, stopChan <-chan struct{}) (<-chan *nomad.StreamFrame, <-chan error) {
	stream, errors := a.NomadClient.AllocFS().Logs(alloc, true, taskName, logType, "start", offset, stopChan, nil)

	return stream, errors
}

func (a *Client) StatFile(alloc *nomad.Allocation, path string) (*nomad.AllocFileInfo, error) {
	data, _, err := a.NomadClient.AllocFS().Stat(alloc, path, nil)

	return data, err
}

func (a *Client) StreamFile(alloc *nomad.Allocation, path string, offset int64, stopChan <-chan struct{}) (<-chan *nomad.StreamFrame, <-chan error) {
	stream, errors := a.NomadClient.AllocFS().Stream(alloc, path, "start", offset, stopChan, nil)

	return stream, errors
}

func readStreamFrame(frames <-chan *nomad.StreamFrame, errs <-chan error) (int, error) {
	select {
	case frame, ok := <-frames:
		if !ok {
			return 0, io.EOF
		}

		return len(frame.Data), nil
	case err := <-errs:
		return 0, err
	}
}