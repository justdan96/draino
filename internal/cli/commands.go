package cli

import (
	"encoding/json"
	"fmt"
	"github.com/DataDog/compute-go/table"
	"github.com/planetlabs/draino/internal/candidate_runner"
	"github.com/planetlabs/draino/internal/groups"
	"github.com/spf13/cobra"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
)

type CLICommands struct {
	ServerAddr *string

	tableOutputParams table.OutputParameters
	outputFormat      outputFormatType
}

func (h *CLICommands) Commands() []*cobra.Command {
	groupCmd := &cobra.Command{
		Use:        "group",
		SuggestFor: []string{"group", "groups"},
		Args:       cobra.MaximumNArgs(1),
		Run:        func(cmd *cobra.Command, args []string) {},
	}

	groupCmd.PersistentFlags().VarP(&h.outputFormat, "output", "o", "output format (table|json)")
	groupCmd.PersistentFlags().BoolVarP(&h.tableOutputParams.NoHeader, "no-header", "", false, "do not display table header")
	groupCmd.PersistentFlags().StringVarP(&h.tableOutputParams.Separator, "separator", "s", "\t|", "column Separator in table output")
	groupCmd.PersistentFlags().IntVarP(&h.tableOutputParams.Padding, "padding", "", 3, "Padding in table output")
	groupCmd.PersistentFlags().StringArrayVarP(&h.tableOutputParams.Sort, "dort", "", nil, "comma separated list of columns for sorting table output")
	groupCmd.PersistentFlags().StringArrayVarP(&h.tableOutputParams.ColumnsVisible, "visible", "", nil, "comma separated list of visible columns for table output")
	groupCmd.PersistentFlags().StringArrayVarP(&h.tableOutputParams.ColumnsHide, "hidden", "", nil, "comma separated list of hidden columns for table output")
	groupCmd.PersistentFlags().StringArrayVarP(&h.tableOutputParams.Filter, "filter", "", nil, "filtering expression for table output")

	groupListCmd := &cobra.Command{
		Use:        "list",
		SuggestFor: []string{"list"},
		Args:       cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return h.cmdGroupList()
		},
	}

	groupCmd.AddCommand(groupListCmd)

	return []*cobra.Command{groupCmd}
}

func ReadFromURL(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("Error: " + err.Error())
		return nil, err
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error: " + err.Error())
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%s\n%s", resp.Status, string(b))
	}
	return b, nil
}

func (h *CLICommands) cmdGroupList() error {
	b, err := ReadFromURL("http://" + *h.ServerAddr + "/groups/list")
	if err != nil {
		return err
	}

	if h.outputFormat == formatJSON {
		fmt.Printf("%s", string(b))
		return nil
	}

	var result []groups.RunnerInfo
	if err := json.Unmarshal(b, &result); err != nil {
		return err
	}

	table := table.NewTable([]string{
		"Group", "Nodes", "Slot", "Filtered", "Warn", "last run", "duration",
	},
		func(obj interface{}) []string {
			item := obj.(groups.RunnerInfo)

			raw, _ := item.Data.Get(candidate_runner.CandidateRunnerInfo)
			var candidateDataInfo candidate_runner.DataInfo
			candidateDataInfo.Import(raw)

			warn := ""
			if candidateDataInfo.NodeCount > 0 {
				if !strings.HasPrefix(candidateDataInfo.Slots, "0") && candidateDataInfo.NodeCount > candidateDataInfo.FilteredOutCount {
					warn = "*"
				}

			}
			return []string{
				string(item.Key),
				fmt.Sprintf("%v", candidateDataInfo.NodeCount),
				fmt.Sprintf("%v", candidateDataInfo.Slots),
				fmt.Sprintf("%v", candidateDataInfo.FilteredOutCount),
				fmt.Sprintf("%s", warn),
				fmt.Sprintf("%v", candidateDataInfo.LastTime),
				fmt.Sprintf("%v", candidateDataInfo.ProcessingDuration),
			}
		})

	for _, s := range result {
		table.Add(s)
	}
	h.tableOutputParams.Apply(table)
	table.Display(os.Stdout)

	return nil
}