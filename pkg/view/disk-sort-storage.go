package view

import (
	"fmt"
	"os"

	"github.com/matst80/slask-finder/pkg/sorting"
)

type DiskOverrideStorage struct {
	path string
}

func DiskPopularityListener(path string) *DiskOverrideStorage {

	return &DiskOverrideStorage{
		path: path,
	}
}

func (s *DiskOverrideStorage) saveToFile(filename string, data string) error {
	filePath := fmt.Sprintf("%s/%s", s.path, filename)
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteString(data)
	if err != nil {
		return err
	}
	return nil
}

func (s *DiskOverrideStorage) PopularityChanged(sort *sorting.SortOverride) error {
	data := sort.ToString()
	return s.saveToFile("item-sort", data)
}

func (s *DiskOverrideStorage) FieldPopularityChanged(sort *sorting.SortOverride) error {
	data := sort.ToString()
	return s.saveToFile("field-sort", data)
}

func (s *DiskOverrideStorage) SessionPopularityChanged(sessionId int64, sort *sorting.SortOverride) error {
	data := sort.ToString()
	return s.saveToFile(fmt.Sprintf("session-items-%d", sessionId), data)
}

func (s *DiskOverrideStorage) SessionFieldPopularityChanged(sessionId int64, sort *sorting.SortOverride) error {
	data := sort.ToString()
	return s.saveToFile(fmt.Sprintf("session-fields-%d", sessionId), data)
}

func (s *DiskOverrideStorage) GroupPopularityChanged(groupId string, sort *sorting.SortOverride) error {
	data := sort.ToString()
	return s.saveToFile(fmt.Sprintf("group-items-%s", groupId), data)
}

func (s *DiskOverrideStorage) GroupFieldPopularityChanged(groupId string, sort *sorting.SortOverride) error {
	data := sort.ToString()
	return s.saveToFile(fmt.Sprintf("group-fields-%s", groupId), data)
}
