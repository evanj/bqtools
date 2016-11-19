package templates

import (
	"bytes"
	"strings"

	bigquery "google.golang.org/api/bigquery/v2"

	"testing"
)

func TestLeastRoundedOne(t *testing.T) {
	tests := [][2]int64{
		// first divisor that has this property is 20: 19/20 rounds to 1.0
		{1, 1},
		{2, 2},
		{19, 19},
		{20, 19},
		{99, 95},
		{100, 95},
		{101, 96},

		// the values we really care about
		{1024, 1024 - 51},
	}

	for i, test := range tests {
		out := leastRoundedOne(test[0])
		if out != test[1] {
			t.Errorf("%d: leastRoundedOne(%d) = %d ; expected %d", i, test[0], out, test[1])
		}
	}
}

func TestHumanBytes(t *testing.T) {
	testData := []struct {
		input  int64
		output string
	}{
		{0, "0 B"},
		{1, "1 B"},
		{972, "972 B"},

		{973, "1.0 KiB"},
		{1024, "1.0 KiB"},
		{1025, "1.0 KiB"},
		{1587, "1.5 KiB"},
		{1024*1024 - 52429, "972.8 KiB"},

		{1024*1024 - 52428, "1.0 MiB"},
		{1024*1024*1024 - 53687092, "972.8 MiB"},

		{1024*1024*1024 - 53687091, "1.0 GiB"},
		{1024 * 1024 * 1024, "1.0 GiB"},
		{1024 * 1024 * 1024 * 1024, "1.0 TiB"},
		{1024 * 1024 * 1024 * 1024 * 1024, "1.0 PiB"},
		{1024 * 1024 * 1024 * 1024 * 1024 * 1024, "1024.0 PiB"},
	}

	for i, test := range testData {
		out := HumanBytes(test.input)
		if out != test.output {
			t.Errorf("%d: HumanBytes(%d) = %s ; expected %s", i, test.input, out, test.output)
		}
	}
}

func TestSelectProject(t *testing.T) {
	// sort of type checks the template (if part of the template doesn't execute, it isn't checked)
	buf := &bytes.Buffer{}
	data := &bigquery.ProjectList{
		Projects: []*bigquery.ProjectListProjects{&bigquery.ProjectListProjects{
			Id: "hello", FriendlyName: "friendly hello",
		}},
	}
	err := SelectProject(buf, data)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), `href="/projects/hello"`) {
		t.Error(buf.String())
	}
	if !strings.Contains(buf.String(), "friendly hello (hello)") {
		t.Error(buf.String())
	}

	// if id == name: omit (id)
	data.Projects[0].FriendlyName = "hello"
	buf.Reset()
	err = SelectProject(buf, data)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "(hello)") {
		t.Error(buf.String())
	}
}

func TestLoading(t *testing.T) {
	buf := &bytes.Buffer{}
	err := Loading(buf, 0, "")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "0%") {
		t.Error(buf.String())
	}
}

func TestProject(t *testing.T) {
	buf := &bytes.Buffer{}
	data := &ProjectData{
		"id", "name", 12345,
		[]*StorageUsage{{12345, "dataset"}},
		[]*StorageUsage{{5000, "table"}},
	}
	err := Project(buf, data)
	if err != nil {
		t.Fatal(err)
	}
	// had a bug that the progress bar wasn't rendered correctly
	if !strings.Contains(buf.String(), `value="41" max="100"`) {
		t.Error(buf.String())
	}
	if !strings.Contains(buf.String(), "name (id)") {
		t.Error(buf.String())
	}
	// check for the table size: ensures we printed them
	if !strings.Contains(buf.String(), "4.9 KiB") {
		t.Error(buf.String())
	}
}
