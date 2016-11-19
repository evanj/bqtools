//go:generate go-bindata -pkg $GOPACKAGE -o=bindata.go -prefix=source source
package templates

import (
	"fmt"
	"html/template"
	"io"
	"math"

	"google.golang.org/api/bigquery/v2"

	"strconv"
)

// https://cloud.google.com/bigquery/pricing#storage
const dollarsPerBytePerMonth = 0.02 / 1024.0 / 1024.0 / 1024.0

// Determine the lowest x such that x/divisor rounded to 1 decimal place == 1.0
func leastRoundedOne(divisor int64) int64 {
	roundUpValue := float64(divisor) * 0.95
	return int64(math.Ceil(roundUpValue))
}

type unit struct {
	divisor         int64
	suffix          string
	leastRoundedOne int64
}

var siByteUnits = [...]unit{
	{1024, "Ki", leastRoundedOne(1024)},
	{1024 * 1024, "Mi", leastRoundedOne(1024 * 1024)},
	{1024 * 1024 * 1024, "Gi", leastRoundedOne(1024 * 1024 * 1024)},
	{1024 * 1024 * 1024 * 1024, "Ti", leastRoundedOne(1024 * 1024 * 1024 * 1024)},
	{1024 * 1024 * 1024 * 1024 * 1024, "Pi", leastRoundedOne(1024 * 1024 * 1024 * 1024 * 1024)},
}

func HumanBytes(bytes int64) string {
	last := unit{1, "", 0}
	for _, byteUnit := range siByteUnits {
		// edge case: we round to one decimal place; if this rounds up:
		if bytes < byteUnit.leastRoundedOne {
			break
		}
		last = byteUnit
	}

	if last.divisor == 1 {
		return strconv.FormatInt(bytes, 10) + " B"
	}
	value := float64(bytes) / float64(last.divisor)
	return strconv.FormatFloat(value, 'f', 1, 64) + " " + last.suffix + "B"
}

func mustEmbeddedTemplate(assetName string) *template.Template {
	asset := string(MustAsset(assetName))
	t := template.New(assetName)
	return template.Must(t.Parse(asset))
}

var index = MustAsset("index.html")
var selectProject = mustEmbeddedTemplate("select_project.html")
var loading = mustEmbeddedTemplate("loading.html")
var project = mustEmbeddedTemplate("project.html")

func Index(w io.Writer) error {
	// currently not a template
	_, err := w.Write(index)
	return err
}

func SelectProject(w io.Writer, data *bigquery.ProjectList) error {
	return selectProject.Execute(w, data)
}

type loadingData struct {
	Percent int
	Message string
}

func Loading(w io.Writer, percent int, message string) error {
	if !(0 <= percent && percent <= 100) {
		return fmt.Errorf("invalid percent: %d", percent)
	}
	return loading.Execute(w, &loadingData{percent, message})
}

type StorageUsage struct {
	Bytes int64
	ID    string
}

func (s *StorageUsage) Percent(total int64) float64 {
	return float64(s.Bytes) * 100.0 / float64(total)
}

func (s *StorageUsage) DollarsPerMonth() float64 {
	return float64(s.Bytes) * dollarsPerBytePerMonth
}

func (s *StorageUsage) HumanBytes() string {
	return HumanBytes(s.Bytes)
}

type ProjectData struct {
	ID             string
	FriendlyName   string
	TotalBytes     int64
	DatasetStorage []*StorageUsage
	TableStorage   []*StorageUsage
}

func (p *ProjectData) TotalCost() float64 {
	return float64(p.TotalBytes) * dollarsPerBytePerMonth
}

func (p *ProjectData) HumanBytes() string {
	return HumanBytes(p.TotalBytes)
}

func Project(w io.Writer, data *ProjectData) error {
	return project.Execute(w, data)
}
